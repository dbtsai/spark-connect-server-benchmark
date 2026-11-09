//! Exercise the server over gRPC through the official Apache Spark Connect Rust client.

use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema};
use arrow::record_batch::RecordBatch;
use spark_connect::functions as f;
use spark_connect::row::{Row, Value};
use spark_connect::{col, lit, DataFrame, DataType, SparkSession, StructField};
use tokio::sync::oneshot;

struct TestServer {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    finished: mpsc::Receiver<()>,
}

impl TestServer {
    fn start() -> Self {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        let thread = thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("server runtime")
                .block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind test server");
                    ready_tx
                        .send(listener.local_addr().expect("listener address"))
                        .expect("test waiting for listener");
                    spark_connect_server::serve(listener, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .expect("serve Spark Connect");
                });
            let _ = finished_tx.send(());
        });
        Self {
            address: ready_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("test server startup"),
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
            finished: finished_rx,
        }
    }

    fn session(&self) -> SparkSession {
        SparkSession::builder()
            .remote(&format!("sc://{}", self.address))
            .get_or_create()
            .expect("connect official Rust client")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            match self.finished.recv_timeout(Duration::from_secs(10)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let result = thread.join();
                    if !thread::panicking() {
                        result.expect("test server shutdown");
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if !thread::panicking() {
                        panic!("test server shutdown exceeded 10 seconds");
                    }
                }
            }
        }
    }
}

fn integers(df: &DataFrame) -> Vec<i64> {
    df.collect()
        .expect("collect integer rows")
        .iter()
        .map(|row| row.get(0).and_then(Value::as_i64).expect("integer value"))
        .collect()
}

fn field(name: &str, data_type: DataType, nullable: bool) -> StructField {
    StructField {
        name: name.to_owned(),
        data_type,
        nullable,
        metadata: Default::default(),
    }
}

#[test]
fn range_transformations_and_aggregates() {
    let server = TestServer::start();
    let spark = server.session();
    let range = spark.range(10).unwrap();
    let transformed = range
        .filter(col("id").ge(lit(3)))
        .select([(col("id") + lit(1)).alias("value")])
        .sort(vec![col("value").desc().expression().clone()])
        .limit(3);
    assert_eq!(integers(&transformed), vec![10, 9, 8]);
    assert_eq!(
        integers(
            &range
                .select([col("*")])
                .sort(vec![col("id").asc().expression().clone()])
        ),
        (0..10).collect::<Vec<_>>()
    );
    assert_eq!(
        integers(
            &range
                .select([col("ID")])
                .sort(vec![col("id").asc().expression().clone()])
        ),
        (0..10).collect::<Vec<_>>()
    );
    assert_eq!(range.count().unwrap(), 10);
    let null = spark_connect::Column::new(spark_connect::Expression::literal(
        spark_connect::LiteralExpression::null(DataType::Long),
    ));
    let cast_and_null = range
        .limit(1)
        .select([
            col("id").cast(DataType::Double).alias("number"),
            null.alias("missing"),
        ])
        .collect()
        .unwrap();
    assert_eq!(cast_and_null[0].get(0), Some(&Value::Double(0.0)));
    assert_eq!(cast_and_null[0].get(1), Some(&Value::Null));
    assert_eq!(
        integers(&range.agg(vec![f::count(col("*")).expression().clone()])),
        vec![10]
    );
    let star = spark_connect::Column::new(spark_connect::Expression::new(
        spark_connect::expression::ExpressionNode::UnresolvedStar(None),
    ));
    assert_eq!(
        integers(&range.agg(vec![f::count(star).expression().clone()])),
        vec![10]
    );
    assert_eq!(
        integers(
            &range
                .limit(4)
                .select_expr(vec!["id * 2 + 1 AS transformed"])
        ),
        vec![1, 3, 5, 7]
    );
    assert_eq!(
        integers(&range.agg(vec![f::sum(col("id")).alias("total").expression().clone()])),
        vec![45]
    );
    let descending = spark
        .range_full(10, 0, -2, None)
        .unwrap()
        .sort(vec![col("id").asc().expression().clone()]);
    assert_eq!(integers(&descending), vec![2, 4, 6, 8, 10]);
    assert_eq!(
        range.schema().unwrap(),
        DataType::Struct {
            fields: vec![field("id", DataType::Long, false)]
        }
    );
    spark.stop().unwrap();
}

#[test]
fn sql_command_schema_and_empty_results() {
    let server = TestServer::start();
    let spark = server.session();
    // The official client sends SqlCommand immediately, then executes its returned relation.
    let query = spark
        .sql("SELECT CAST(42 AS BIGINT) AS answer, CAST(NULL AS BIGINT) AS missing")
        .unwrap();
    let rows = query.collect().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).and_then(Value::as_i64), Some(42));
    assert_eq!(rows[0].get(1), Some(&Value::Null));
    let cast = query
        .select([col("missing").cast(DataType::Long)])
        .collect()
        .unwrap();
    assert_eq!(cast[0].get(0), Some(&Value::Null));
    let DataType::Struct { fields } = query.schema().unwrap() else {
        panic!("SQL schema should be a struct");
    };
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name, "answer");
    assert_eq!(fields[0].data_type, DataType::Long);
    assert_eq!(fields[1].name, "missing");
    assert_eq!(fields[1].data_type, DataType::Long);
    assert!(fields[1].nullable);

    let view_string = spark
        .sql("SELECT arrow_cast('abc', 'Utf8View') AS s")
        .unwrap();
    let batches = view_string.collect_record_batches().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].column(0).data_type(), &ArrowType::LargeUtf8);
    let rows = view_string.collect().unwrap();
    assert_eq!(rows[0].get(0).and_then(Value::as_str), Some("abc"));

    let view_binary = spark
        .sql("SELECT arrow_cast(arrow_cast('hi', 'Binary'), 'BinaryView') AS b")
        .unwrap();
    let batches = view_binary.collect_record_batches().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].column(0).data_type(), &ArrowType::LargeBinary);
    let rows = view_binary.collect().unwrap();
    assert_eq!(
        rows[0].get(0).and_then(Value::as_bytes),
        Some(b"hi".as_slice())
    );

    let empty = spark.range(0).unwrap();
    assert!(empty.collect().unwrap().is_empty());
    assert_eq!(empty.count().unwrap(), 0);
    assert_eq!(
        empty.schema().unwrap(),
        DataType::Struct {
            fields: vec![field("id", DataType::Long, false)]
        }
    );
    spark.stop().unwrap();
}

#[test]
fn typed_local_rows_and_arrow_batches_preserve_nulls() {
    let server = TestServer::start();
    let spark = server.session();
    let schema = DataType::Struct {
        fields: vec![
            field("id", DataType::Long, false),
            field(
                "label",
                DataType::String {
                    collation: "UTF8_BINARY".into(),
                },
                true,
            ),
        ],
    };
    let rows = vec![
        Row::new(
            vec!["id".into(), "label".into()],
            vec![Value::Long(2), Value::Null],
        ),
        Row::new(
            vec!["id".into(), "label".into()],
            vec![Value::Long(1), Value::String("one".into())],
        ),
        Row::new(
            vec!["id".into(), "label".into()],
            vec![Value::Long(3), Value::String("one".into())],
        ),
    ];
    let local = spark.create_dataframe(rows, schema.clone()).unwrap();
    assert_eq!(local.schema().unwrap(), schema);
    let renamed = local.to_df(vec!["number", "text"]);
    assert_eq!(renamed.columns().unwrap(), vec!["number", "text"]);
    let renamed_rows = renamed
        .sort(vec![col("number").asc().expression().clone()])
        .collect()
        .unwrap();
    assert_eq!(renamed_rows[0].get(0).and_then(Value::as_i64), Some(1));
    assert_eq!(renamed_rows[0].get(1).and_then(Value::as_str), Some("one"));
    assert!(local.to_df(vec!["too_few"]).collect().is_err());
    assert!(local
        .to_df(vec!["duplicate", "duplicate"])
        .collect()
        .is_err());
    let collected = local
        .sort(vec![col("id").asc().expression().clone()])
        .collect()
        .unwrap();
    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0].get(0).and_then(Value::as_i64), Some(1));
    assert_eq!(collected[0].get(1).and_then(Value::as_str), Some("one"));
    assert_eq!(collected[1].get(1), Some(&Value::Null));
    let grouped = local
        .group_by(["label"])
        .agg(vec![f::sum(col("id")).alias("total").expression().clone()])
        .sort(vec![col("label").asc_nulls_first().expression().clone()])
        .collect()
        .unwrap();
    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[0].get(0), Some(&Value::Null));
    assert_eq!(grouped[0].get(1).and_then(Value::as_i64), Some(2));
    assert_eq!(grouped[1].get(0).and_then(Value::as_str), Some("one"));
    assert_eq!(grouped[1].get(1).and_then(Value::as_i64), Some(4));

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowType::Int64, false),
        Field::new("label", ArrowType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec![Some("three"), None])),
        ],
    )
    .unwrap();
    let arrow_df = spark
        .create_dataframe_from_arrow(vec![batch], None)
        .unwrap();
    let batches = arrow_df
        .sort(vec![col("id").asc().expression().clone()])
        .collect_record_batches()
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let rows = arrow_df
        .sort(vec![col("id").asc().expression().clone()])
        .collect()
        .unwrap();
    assert_eq!(rows[0].get(0).and_then(Value::as_i64), Some(3));
    assert_eq!(rows[0].get(1).and_then(Value::as_str), Some("three"));
    assert_eq!(rows[1].get(1), Some(&Value::Null));

    let capitalized = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("Id", ArrowType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![2, 1, 2]))],
    )
    .unwrap();
    let capitalized = spark
        .create_dataframe_from_arrow(vec![capitalized], None)
        .unwrap();
    assert_eq!(
        integers(
            &capitalized
                .select([col("Id")])
                .sort(vec![col("Id").asc().expression().clone()])
        ),
        vec![1, 2, 2]
    );
    assert_eq!(
        integers(
            &capitalized
                .select([col("id")])
                .sort(vec![col("Id").asc().expression().clone()])
        ),
        vec![1, 2, 2]
    );
    let grouped = capitalized
        .group_by(["Id"])
        .agg(vec![f::count(lit(1)).alias("total").expression().clone()])
        .sort(vec![col("Id").asc().expression().clone()])
        .collect()
        .unwrap();
    assert_eq!(grouped.len(), 2);
    assert_eq!(grouped[0].get(0).and_then(Value::as_i64), Some(1));
    assert_eq!(grouped[0].get(1).and_then(Value::as_i64), Some(1));
    assert_eq!(grouped[1].get(0).and_then(Value::as_i64), Some(2));
    assert_eq!(grouped[1].get(1).and_then(Value::as_i64), Some(2));

    let ambiguous = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", ArrowType::Int64, false),
            Field::new("iD", ArrowType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![11])),
            Arc::new(Int64Array::from(vec![21])),
        ],
    )
    .unwrap();
    let ambiguous = spark
        .create_dataframe_from_arrow(vec![ambiguous], None)
        .unwrap();
    assert_eq!(integers(&ambiguous.select([col("Id")])), vec![11]);
    assert_eq!(integers(&ambiguous.select([col("iD")])), vec![21]);
    let error = ambiguous.select([col("ID")]).collect().unwrap_err();
    assert!(
        error.to_string().to_lowercase().contains("ambiguous"),
        "{error}"
    );

    let empty_local = spark.create_dataframe(Vec::new(), schema.clone()).unwrap();
    assert!(empty_local.collect().unwrap().is_empty());
    assert_eq!(empty_local.schema().unwrap(), schema);
    let named_like_function = spark
        .create_dataframe(
            vec![Row::new(vec!["current_date".into()], vec![Value::Long(17)])],
            DataType::Struct {
                fields: vec![field("current_date", DataType::Long, false)],
            },
        )
        .unwrap();
    assert_eq!(
        integers(&named_like_function.select([col("current_date")])),
        vec![17]
    );
    spark.stop().unwrap();
}

#[test]
fn large_string_results_fit_the_official_clients_grpc_limit() {
    let server = TestServer::start();
    let spark = server.session();
    let rows = spark
        .sql("SELECT repeat('x', 2000) AS s FROM range(8192)")
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(rows.len(), 8192);
    assert!(rows.iter().all(|row| row
        .get(0)
        .and_then(Value::as_str)
        .is_some_and(|value| value.len() == 2000 && value.bytes().all(|byte| byte == b'x'))));
    spark.stop().unwrap();
}

#[test]
fn temporary_views_and_configuration_are_session_scoped() {
    let server = TestServer::start();
    let spark = server.session();
    let other = spark.new_session();
    spark
        .range(4)
        .unwrap()
        .create_or_replace_temp_view("numbers")
        .unwrap();
    assert_eq!(spark.table("numbers").unwrap().count().unwrap(), 4);
    assert_eq!(
        integers(&spark.sql("SELECT SUM(id) AS total FROM numbers").unwrap()),
        vec![6]
    );
    assert!(other.table("numbers").unwrap().collect().is_err());

    spark
        .range(2)
        .unwrap()
        .create_or_replace_temp_view("numbers")
        .unwrap();
    assert_eq!(spark.table("numbers").unwrap().count().unwrap(), 2);
    assert!(spark
        .conf()
        .set("spark.sql.session.timeZone", "America/Los_Angeles")
        .is_err());
    assert_eq!(
        spark
            .conf()
            .get("spark.sql.session.timeZone")
            .unwrap()
            .as_deref(),
        Some("UTC")
    );

    spark.conf().set("test.session.setting", "first").unwrap();
    assert_eq!(
        spark.conf().get("test.session.setting").unwrap().as_deref(),
        Some("first")
    );
    assert_eq!(
        other
            .conf()
            .get_with_default("test.session.setting", Some("unset"))
            .unwrap()
            .as_deref(),
        Some("unset")
    );
    spark.stop().unwrap();
    assert_eq!(other.range(2).unwrap().count().unwrap(), 2);
    other.stop().unwrap();
}

#[test]
fn unsupported_relations_return_an_error() {
    let server = TestServer::start();
    let spark = server.session();
    let error = spark
        .range(10)
        .unwrap()
        .sample(0.5, Some(1))
        .collect()
        .unwrap_err();
    let message = error.to_string().to_lowercase();
    assert!(
        message.contains("sample") || message.contains("unimplemented"),
        "{error}"
    );
    let decimals = spark
        .sql("SELECT CAST(1 AS DECIMAL(10, 2)) AS value")
        .unwrap();
    let error = decimals
        .select([col("value") / lit(2)])
        .collect()
        .unwrap_err();
    assert!(error.to_string().contains("decimal division"), "{error}");
    // A rejected plan must leave the same session usable.
    assert_eq!(spark.range(2).unwrap().count().unwrap(), 2);
    spark.stop().unwrap();
}
