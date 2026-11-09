//! Deterministic, read-only plans and Arrow payloads built before timed RPCs.
//!
//! Relation plans use the pinned fork Rust client's public logical-plan builders.
//! This corpus covers representative shapes and types; it does not claim every
//! relation or expression variant in Spark Connect.

use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::builder::{Int64Builder, MapFieldNames, StringBuilder};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, MapBuilder, NullArray,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType as ArrowType, Field, Int32Type, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use prost::Message;
use serde_json::{json, Value};
use spark_connect::expression::{Expression, LiteralExpression, NullOrdering, SortOrder};
use spark_connect::functions as f;
use spark_connect::plan::{self, AggregateGroupType, JoinType, LogicalPlan, SetOpType};
use spark_connect::window::Window;
use spark_connect::{col, lit, Column, DataType as SparkType, StructField};
use spark_connect_proto as proto;

#[derive(Clone)]
pub struct PlanCase {
    pub name: String,
    pub relation: proto::Relation,
    pub expected_schema: proto::DataType,
    pub metadata: Value,
}

struct Scale {
    chains: &'static [usize],
    widths: &'static [usize],
    join_levels: &'static [usize],
    union_leaves: &'static [usize],
    payload_sizes: &'static [usize],
    literal_sizes: &'static [usize],
}

impl Scale {
    fn for_profile(profile: &str) -> Result<Self, String> {
        Ok(match profile {
            "smoke" => Self {
                chains: &[4],
                widths: &[8],
                join_levels: &[2],
                union_leaves: &[4],
                payload_sizes: &[0, 4096],
                literal_sizes: &[128],
            },
            "standard" => Self {
                chains: &[1, 8, 16],
                widths: &[1, 32, 128],
                join_levels: &[1, 3],
                union_leaves: &[2, 8, 16],
                payload_sizes: &[0, 4096, 65536, 1048576],
                literal_sizes: &[0, 1024, 65536, 1048576],
            },
            "stress" => Self {
                // Generated recursive messages also consume significant stack.
                // This cap is checked on a normal Rust test thread, not only
                // against protobuf's numerical recursion limit.
                chains: &[1, 8, 16, 20],
                widths: &[1, 32, 128, 1024],
                join_levels: &[1, 3, 5],
                union_leaves: &[2, 8, 16, 20],
                payload_sizes: &[0, 4096, 65536, 1048576, 16777216],
                literal_sizes: &[0, 1024, 65536, 1048576, 16777216],
            },
            _ => {
                return Err(format!(
                    "unknown corpus profile {profile:?}; use smoke, standard, or stress"
                ))
            }
        })
    }
}

/// Build all plans/payloads outside the timed load-generation loop.
pub fn build(profile: &str) -> Result<Vec<PlanCase>, String> {
    let scale = Scale::for_profile(profile)?;
    let id_schema = struct_type(vec![field("id", SparkType::Long, false)]);
    let mut cases = vec![case(
        "range",
        "range",
        plan::range(0, 1000, 1),
        id_schema.clone(),
        json!({"input_rows": 1000}),
    )];

    for &depth in scale.chains {
        let mut input = plan::range(0, 1000, 1);
        for index in 0..depth {
            input = if index % 2 == 0 {
                plan::project(input, [col("id")])
            } else {
                plan::filter(input, col("id").ge(lit(index as i64)))
            };
        }
        cases.push(case(
            &format!("chain_{depth}"),
            "project_filter_chain",
            input,
            id_schema.clone(),
            json!({"transformations": depth, "input_rows": 1000}),
        ));
    }
    for &width in scale.widths {
        let columns = (0..width).map(|i| (col("id") + lit(i as i64)).alias(&format!("c{i}")));
        let schema = struct_type(
            (0..width)
                .map(|i| field(&format!("c{i}"), SparkType::Long, false))
                .collect(),
        );
        cases.push(case(
            &format!("wide_{width}"),
            "wide_projection",
            plan::project(plan::range(0, 1000, 1), columns),
            schema,
            json!({"projection_width": width, "input_rows": 1000}),
        ));
    }
    for &levels in scale.join_levels {
        cases.push(case(
            &format!("balanced_join_{}", 1usize << levels),
            "balanced_join",
            balanced_join(levels),
            id_schema.clone(),
            json!({"join_leaves": 1usize << levels, "join_type": "inner_using_id"}),
        ));
    }
    for &leaves in scale.union_leaves {
        let mut input = plan::range(0, 1000, 1);
        for _ in 1..leaves {
            input = plan::set_operation(
                input,
                plan::range(0, 1000, 1),
                SetOpType::Union,
                true,
                false,
                false,
            );
        }
        cases.push(case(
            &format!("union_{leaves}"),
            "left_deep_union",
            input,
            id_schema.clone(),
            json!({"union_leaves": leaves, "union_all": true}),
        ));
    }
    cases.push(case(
        "aggregate",
        "aggregate",
        plan::aggregate(
            plan::range(0, 1000, 1),
            AggregateGroupType::GroupBy,
            vec![],
            vec![
                f::count(lit(1_i64)).alias("n").expression().clone(),
                f::sum(col("id")).alias("total").expression().clone(),
            ],
        ),
        struct_type(vec![
            field("n", SparkType::Long, false),
            field("total", SparkType::Long, true),
        ]),
        json!({"input_rows": 1000}),
    ));
    let window = Window::order_by(vec![SortOrder {
        child: Expression::column("id"),
        ascending: true,
        null_ordering: NullOrdering::First,
    }]);
    cases.push(case(
        "window_sort",
        "window_sort",
        plan::sort(
            plan::project(
                plan::range(0, 1000, 1),
                [col("id"), f::row_number().over(window).alias("row_number")],
            ),
            vec![col("id").desc().expression().clone()],
            true,
        ),
        struct_type(vec![
            field("id", SparkType::Long, false),
            field("row_number", SparkType::Integer, false),
        ]),
        json!({"input_rows": 1000}),
    ));
    cases.push(case(
        "sql",
        "sql",
        plan::sql("SELECT id FROM range(1000) WHERE id % 7 = 0"),
        id_schema,
        json!({"input_rows": 1000, "sql_parse": true}),
    ));
    cases.push(typed_literals());

    for &size in scale.literal_sizes {
        for binary in [false, true] {
            let (value, ty, family) = if binary {
                (
                    LiteralExpression::Binary((0..size).map(|i| (i % 251) as u8).collect()),
                    SparkType::Binary,
                    "binary",
                )
            } else {
                (
                    LiteralExpression::String("x".repeat(size)),
                    string_type(),
                    "string",
                )
            };
            let value = Column::new(Expression::literal(value)).alias("value");
            cases.push(case(
                &format!("literal_{family}_{size}"),
                "large_literal",
                plan::project(plan::range(0, 1, 1), [value]),
                struct_type(vec![field("value", ty, false)]),
                json!({"literal_type": family, "literal_bytes": size, "input_rows": 1}),
            ));
        }
    }
    for family in [
        "primitive",
        "variable",
        "decimal_temporal",
        "nested",
        "map",
        "null_heavy",
    ] {
        for &target in scale.payload_sizes {
            cases.push(local_case(family, target)?);
        }
    }
    Ok(cases)
}

/// Export reproducible request plans and bounded prebuilt result payloads.
///
/// Local Arrow inputs larger than 8 MiB remain request cases, but are explicitly
/// excluded from fixture result replay. Result payloads are bounded to 64 MiB in
/// total. Existing files cause an error instead of silently changing a fixture.
pub fn export(cases: &[PlanCase], directory: &Path) -> Result<Value, String> {
    fs::create_dir_all(directory).map_err(|e| format!("create corpus directory: {e}"))?;
    let mut entries = vec![];
    let mut payloads = vec![];
    let mut replay_bytes = 0usize;
    for case in cases {
        if case.name.is_empty()
            || !case
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(format!("unsafe corpus case name {:?}", case.name));
        }
        let relation_file = format!("{}.relation.pb", case.name);
        let schema_file = format!("{}.schema.pb", case.name);
        write_new(
            &directory.join(&relation_file),
            &case.relation.encode_to_vec(),
        )?;
        write_new(
            &directory.join(&schema_file),
            &case.expected_schema.encode_to_vec(),
        )?;
        let mut exclusion = None;
        let mut included = false;
        if let Some(proto::relation::RelType::LocalRelation(local)) = &case.relation.rel_type {
            if let Some(bytes) = &local.data {
                if bytes.len() > 8 * 1024 * 1024 {
                    exclusion = Some("input exceeds 8 MiB fixture response-payload limit");
                } else if replay_bytes + bytes.len() > 64 * 1024 * 1024 - 4096 {
                    exclusion = Some("fixture response-payload total would exceed 64 MiB");
                } else {
                    let arrow_file = format!("{}.arrow", case.name);
                    write_new(&directory.join(&arrow_file), bytes)?;
                    payloads.push(json!({"key": case.name,
                        "rows": case.metadata["details"]["input_rows"],
                        "schema_file": schema_file, "arrow_file": arrow_file}));
                    replay_bytes += bytes.len();
                    included = true;
                }
            }
        }
        entries.push(json!({"key": case.name, "relation_file": relation_file,
            "expected_schema_file": schema_file, "metadata": case.metadata,
            "response_payload_included": included, "response_payload_exclusion": exclusion}));
    }
    // A stable one-row response for shape-only requests and protocol probes.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", ArrowType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![0_i64]))],
    )
    .map_err(|e| e.to_string())?;
    let mut bytes = vec![];
    {
        let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref())
            .map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.finish().map_err(|e| e.to_string())?;
    }
    write_new(&directory.join("payload-range.arrow"), &bytes)?;
    let fallback_schema = struct_type(vec![field("id", SparkType::Long, false)]).to_proto();
    write_new(
        &directory.join("payload-range.schema.pb"),
        &fallback_schema.encode_to_vec(),
    )?;
    payloads.push(json!({"key": "range", "rows": 1,
        "schema_file": "payload-range.schema.pb", "arrow_file": "payload-range.arrow"}));
    replay_bytes += bytes.len();
    let manifest = json!({"version": 1, "cases": entries, "payloads": payloads,
        "rust_api_audit": crate::api_audit::audit()?,
        "response_payload_bytes": replay_bytes, "response_payload_limit_bytes": 8 * 1024 * 1024,
        "response_total_limit_bytes": 64 * 1024 * 1024,
        "result_semantics": "prebuilt fixture replay; no plan evaluation"});
    write_new(
        &directory.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)
            .map_err(|e| e.to_string())?
            .as_slice(),
    )?;
    Ok(manifest)
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| format!("create {}: {e}", path.display()))?;
    output
        .write_all(bytes)
        .map_err(|e| format!("write {}: {e}", path.display()))
}

fn balanced_join(levels: usize) -> LogicalPlan {
    if levels == 0 {
        return plan::range(0, 1000, 1);
    }
    plan::join(
        balanced_join(levels - 1),
        balanced_join(levels - 1),
        JoinType::Inner,
        None,
        vec!["id".into()],
    )
}

fn field(name: &str, data_type: SparkType, nullable: bool) -> StructField {
    StructField {
        name: name.into(),
        data_type,
        nullable,
        metadata: Default::default(),
    }
}
fn struct_type(fields: Vec<StructField>) -> SparkType {
    SparkType::Struct { fields }
}
fn string_type() -> SparkType {
    SparkType::String {
        collation: "UTF8_BINARY".into(),
    }
}

pub(crate) fn raw_typed_literals() -> PlanCase {
    let values = vec![
        (
            "boolean",
            LiteralExpression::Boolean(true),
            SparkType::Boolean,
            false,
        ),
        ("byte", LiteralExpression::Byte(-7), SparkType::Byte, false),
        (
            "short",
            LiteralExpression::Short(-300),
            SparkType::Short,
            false,
        ),
        (
            "integer",
            LiteralExpression::Integer(12345),
            SparkType::Integer,
            false,
        ),
        (
            "long",
            LiteralExpression::Long(9007199254740993),
            SparkType::Long,
            false,
        ),
        (
            "float",
            LiteralExpression::Float(1.25),
            SparkType::Float,
            false,
        ),
        (
            "double",
            LiteralExpression::Double(-2.5),
            SparkType::Double,
            false,
        ),
        (
            "decimal",
            LiteralExpression::Decimal {
                value: "12345.67".into(),
                precision: 12,
                scale: 2,
            },
            SparkType::Decimal {
                precision: 12,
                scale: 2,
            },
            false,
        ),
        (
            "string",
            LiteralExpression::String("π😀".into()),
            string_type(),
            false,
        ),
        (
            "binary",
            LiteralExpression::Binary(vec![0, 255, 127]),
            SparkType::Binary,
            false,
        ),
        (
            "date",
            LiteralExpression::Date(20000),
            SparkType::Date,
            false,
        ),
        (
            "timestamp",
            LiteralExpression::Timestamp(1700000000123456),
            SparkType::Timestamp,
            false,
        ),
        (
            "timestamp_ntz",
            LiteralExpression::TimestampNtz(1700000000123456),
            SparkType::TimestampNtz,
            false,
        ),
        (
            "null_long",
            LiteralExpression::Null(SparkType::Long),
            SparkType::Long,
            true,
        ),
        (
            "array",
            LiteralExpression::Array {
                element_type: Box::new(SparkType::Integer),
                elements: vec![
                    LiteralExpression::Integer(1),
                    LiteralExpression::Null(SparkType::Integer),
                    LiteralExpression::Integer(3),
                ],
            },
            SparkType::Array {
                element_type: Box::new(SparkType::Integer),
                contains_null: true,
            },
            false,
        ),
    ];
    let schema = struct_type(
        values
            .iter()
            .map(|(name, _, ty, nullable)| field(name, ty.clone(), *nullable))
            .collect(),
    );
    let columns = values
        .into_iter()
        .map(|(name, value, _, _)| Column::new(Expression::literal(value)).alias(name));
    case(
        "typed_literals",
        "typed_literals",
        plan::project(plan::range(0, 1, 1), columns),
        schema,
        json!({"input_rows": 1, "literal_data_type_fields": "official_client_unmodified"}),
    )
}

fn typed_literals() -> PlanCase {
    // The timed case intentionally matches the raw output inspected by the client audit.
    raw_typed_literals()
}

fn case(
    name: &str,
    family: &str,
    plan: LogicalPlan,
    schema: SparkType,
    details: Value,
) -> PlanCase {
    let relation = plan.to_proto();
    let mut stats = Stats::default();
    visit_relation(&relation, 1, &mut stats);
    let mut types = BTreeSet::new();
    type_tags(&schema, &mut types);
    let columns = match &schema {
        SparkType::Struct { fields } => fields.len(),
        _ => 0,
    };
    PlanCase {
        name: name.into(),
        expected_schema: schema.to_proto(),
        metadata: json!({"family": family, "origin": "official_rust_logical_plan",
            "relation_nodes": stats.nodes, "relation_depth": stats.depth,
            "expression_nodes": stats.expressions, "columns": columns,
            "relation_tags": stats.relations, "expression_tags": stats.expression_tags,
            "literal_tags": stats.literal_tags, "data_types": types,
            "encoded_relation_bytes": relation.encoded_len(), "details": details}),
        relation,
    }
}

fn local_case(family: &str, target: usize) -> Result<PlanCase, String> {
    let row_estimate = match family {
        "primitive" => 32,
        "variable" => 144,
        "decimal_temporal" => 40,
        "nested" => 80,
        "map" => 56,
        "null_heavy" => 24,
        _ => unreachable!(),
    };
    let mut rows = if target == 0 {
        0
    } else {
        (target.saturating_sub(2048) / row_estimate).clamp(1, 2_000_000)
    };
    let (batch, bytes) = loop {
        let batch = make_batch(family, rows)?;
        let mut bytes = vec![];
        {
            let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref())
                .map_err(|e| e.to_string())?;
            writer.write(&batch).map_err(|e| e.to_string())?;
            writer.finish().map_err(|e| e.to_string())?;
        }
        if target == 0 || bytes.len() <= target || rows <= 1 {
            break (batch, bytes);
        }
        rows = ((rows as u128 * target as u128 * 99 / bytes.len() as u128 / 100) as usize).max(1);
    };
    let payload_bytes = bytes.len();
    let schema = arrow_schema_to_spark(batch.schema().as_ref())?;
    // Inference from IPC avoids a separate server-side cast projection. The
    // official LogicalPlan variant supports an optional schema for this purpose.
    let plan = LogicalPlan::LocalRelation {
        schema: None,
        data: Some(bytes),
    };
    Ok(case(
        &format!("local_{family}_{target}"),
        "local_arrow",
        plan,
        schema,
        json!({"payload_kind": family, "target_ipc_bytes": target, "arrow_ipc_bytes": payload_bytes,
            "input_rows": rows, "null_pattern": if family == "null_heavy" { "90_percent_null" } else { "every_seventh_row_null" },
            "schema_inferred_from_arrow": true}),
    ))
}

fn make_batch(family: &str, rows: usize) -> Result<RecordBatch, String> {
    let columns: Vec<(&str, ArrayRef)> = match family {
        "primitive" => vec![
            (
                "boolean",
                Arc::new(BooleanArray::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i % 2 == 0)),
                )),
            ),
            (
                "byte",
                Arc::new(Int8Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some((i % 127) as i8)),
                )),
            ),
            (
                "short",
                Arc::new(Int16Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some((i % 32000) as i16)),
                )),
            ),
            (
                "integer",
                Arc::new(Int32Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i as i32)),
                )),
            ),
            (
                "long",
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i as i64 * -101)),
                )),
            ),
            (
                "float",
                Arc::new(Float32Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i as f32 / 8.0)),
                )),
            ),
            (
                "double",
                Arc::new(Float64Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(i as f64 / -16.0)),
                )),
            ),
            ("null", Arc::new(NullArray::new(rows))),
        ],
        "variable" => {
            let text = "π😀abc".repeat(8);
            let data = (0..64).map(|i| i as u8 * 3).collect::<Vec<_>>();
            vec![
                (
                    "string",
                    Arc::new(StringArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(text.as_str())),
                    )),
                ),
                (
                    "binary",
                    Arc::new(BinaryArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(data.as_slice())),
                    )),
                ),
            ]
        }
        "decimal_temporal" => vec![
            (
                "decimal",
                Arc::new(
                    Decimal128Array::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(i as i128 * -123456)),
                    )
                    .with_precision_and_scale(28, 6)
                    .map_err(|e| e.to_string())?,
                ),
            ),
            (
                "date",
                Arc::new(Date32Array::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(20000 + (i % 365) as i32)),
                )),
            ),
            (
                "timestamp",
                Arc::new(
                    TimestampMicrosecondArray::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(1700000000000000 + i as i64)),
                    )
                    .with_timezone("UTC"),
                ),
            ),
            (
                "timestamp_ntz",
                Arc::new(TimestampMicrosecondArray::from_iter(
                    (0..rows).map(|i| (i % 7 != 0).then_some(1700000000000000 + i as i64)),
                )),
            ),
        ],
        "nested" => {
            let number: ArrayRef =
                Arc::new(Int64Array::from_iter((0..rows).map(|i| Some(i as i64))));
            let text: ArrayRef = Arc::new(StringArray::from_iter(
                (0..rows).map(|i| (i % 7 != 0).then_some("nested π😀")),
            ));
            let items: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                (0..rows).map(|i| (i % 7 != 0).then_some(vec![Some(i as i32), None, Some(-1)])),
            ));
            let fields = vec![
                Arc::new(Field::new("number", number.data_type().clone(), true)),
                Arc::new(Field::new("text", text.data_type().clone(), true)),
                Arc::new(Field::new("items", items.data_type().clone(), true)),
            ]
            .into();
            let nested = StructArray::new(
                fields,
                vec![number, text, items],
                Some(NullBuffer::from(
                    (0..rows).map(|i| i % 7 != 0).collect::<Vec<_>>(),
                )),
            );
            vec![("record", Arc::new(nested))]
        }
        "map" => {
            // Arrow Java's MapVector and Spark's MapAccessor use these exact
            // child names. Arrow Rust defaults to plural "keys"/"values".
            let mut builder = MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".into(),
                    key: "key".into(),
                    value: "value".into(),
                }),
                StringBuilder::new(),
                Int64Builder::new(),
            );
            for i in 0..rows {
                if i % 7 == 0 {
                    builder.append(false).map_err(|e| e.to_string())?;
                    continue;
                }
                builder.keys().append_value("alpha");
                builder.values().append_value(i as i64);
                builder.keys().append_value("π😀");
                builder.values().append_null();
                builder.append(true).map_err(|e| e.to_string())?;
            }
            vec![("mapping", Arc::new(builder.finish()))]
        }
        "null_heavy" => vec![
            (
                "long",
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| (i % 10 == 0).then_some(i as i64)),
                )),
            ),
            (
                "double",
                Arc::new(Float64Array::from_iter(
                    (0..rows).map(|i| (i % 10 == 0).then_some(i as f64 / 2.0)),
                )),
            ),
            (
                "string",
                Arc::new(StringArray::from_iter(
                    (0..rows).map(|i| (i % 10 == 0).then_some("nullable π😀")),
                )),
            ),
            ("null", Arc::new(NullArray::new(rows))),
        ],
        _ => return Err(format!("unknown Arrow payload family {family}")),
    };
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(
        schema,
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .map_err(|e| e.to_string())
}

fn arrow_schema_to_spark(schema: &Schema) -> Result<SparkType, String> {
    Ok(struct_type(
        schema
            .fields()
            .iter()
            .map(|f| {
                Ok(field(
                    f.name(),
                    arrow_type_to_spark(f.data_type())?,
                    f.is_nullable(),
                ))
            })
            .collect::<Result<_, String>>()?,
    ))
}
fn arrow_type_to_spark(ty: &ArrowType) -> Result<SparkType, String> {
    Ok(match ty {
        ArrowType::Null => SparkType::Null,
        ArrowType::Boolean => SparkType::Boolean,
        ArrowType::Int8 => SparkType::Byte,
        ArrowType::Int16 => SparkType::Short,
        ArrowType::Int32 => SparkType::Integer,
        ArrowType::Int64 => SparkType::Long,
        ArrowType::Float32 => SparkType::Float,
        ArrowType::Float64 => SparkType::Double,
        ArrowType::Utf8 => string_type(),
        ArrowType::Binary => SparkType::Binary,
        ArrowType::Decimal128(precision, scale) => SparkType::Decimal {
            precision: i32::from(*precision),
            scale: i32::from(*scale),
        },
        ArrowType::Date32 => SparkType::Date,
        ArrowType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz) => {
            if tz.is_some() {
                SparkType::Timestamp
            } else {
                SparkType::TimestampNtz
            }
        }
        ArrowType::Struct(fields) => struct_type(
            fields
                .iter()
                .map(|f| {
                    Ok(field(
                        f.name(),
                        arrow_type_to_spark(f.data_type())?,
                        f.is_nullable(),
                    ))
                })
                .collect::<Result<_, String>>()?,
        ),
        ArrowType::List(child) => SparkType::Array {
            element_type: Box::new(arrow_type_to_spark(child.data_type())?),
            contains_null: child.is_nullable(),
        },
        ArrowType::Map(entries, _) => {
            let ArrowType::Struct(fields) = entries.data_type() else {
                return Err("map entries are not a struct".into());
            };
            SparkType::Map {
                key_type: Box::new(arrow_type_to_spark(fields[0].data_type())?),
                value_type: Box::new(arrow_type_to_spark(fields[1].data_type())?),
                value_contains_null: fields[1].is_nullable(),
            }
        }
        other => return Err(format!("unsupported corpus Arrow type {other}")),
    })
}

fn type_tags(ty: &SparkType, tags: &mut BTreeSet<&'static str>) {
    let name = match ty {
        SparkType::Null => "null",
        SparkType::Boolean => "boolean",
        SparkType::Byte => "byte",
        SparkType::Short => "short",
        SparkType::Integer => "integer",
        SparkType::Long => "long",
        SparkType::Float => "float",
        SparkType::Double => "double",
        SparkType::Decimal { .. } => "decimal",
        SparkType::String { .. } => "string",
        SparkType::Binary => "binary",
        SparkType::Date => "date",
        SparkType::Timestamp => "timestamp",
        SparkType::TimestampNtz => "timestamp_ntz",
        SparkType::Array { element_type, .. } => {
            type_tags(element_type, tags);
            "array"
        }
        SparkType::Map {
            key_type,
            value_type,
            ..
        } => {
            type_tags(key_type, tags);
            type_tags(value_type, tags);
            "map"
        }
        SparkType::Struct { fields } => {
            for field in fields {
                type_tags(&field.data_type, tags);
            }
            "struct"
        }
        _ => "other",
    };
    tags.insert(name);
}

#[derive(Default)]
struct Stats {
    nodes: usize,
    depth: usize,
    expressions: usize,
    relations: BTreeSet<&'static str>,
    expression_tags: BTreeSet<&'static str>,
    literal_tags: BTreeSet<&'static str>,
}

fn visit_relation(relation: &proto::Relation, depth: usize, stats: &mut Stats) {
    use proto::relation::RelType as R;
    stats.nodes += 1;
    stats.depth = stats.depth.max(depth);
    let mut children: Vec<&proto::Relation> = vec![];
    let name = match relation.rel_type.as_ref() {
        Some(R::Range(_)) => "range",
        Some(R::Sql(_)) => "sql",
        Some(R::LocalRelation(_)) => "local_relation",
        Some(R::Project(p)) => {
            children.extend(p.input.as_deref());
            for e in &p.expressions {
                visit_expression(e, stats);
            }
            "project"
        }
        Some(R::Filter(p)) => {
            children.extend(p.input.as_deref());
            if let Some(e) = &p.condition {
                visit_expression(e, stats);
            }
            "filter"
        }
        Some(R::Join(p)) => {
            children.extend(p.left.as_deref());
            children.extend(p.right.as_deref());
            if let Some(e) = &p.join_condition {
                visit_expression(e, stats);
            }
            "join"
        }
        Some(R::SetOp(p)) => {
            children.extend(p.left_input.as_deref());
            children.extend(p.right_input.as_deref());
            "set_op"
        }
        Some(R::Aggregate(p)) => {
            children.extend(p.input.as_deref());
            for e in p
                .grouping_expressions
                .iter()
                .chain(&p.aggregate_expressions)
            {
                visit_expression(e, stats);
            }
            "aggregate"
        }
        Some(R::Sort(p)) => {
            children.extend(p.input.as_deref());
            for e in &p.order {
                if let Some(e) = e.child.as_deref() {
                    visit_expression(e, stats);
                }
            }
            stats.expression_tags.insert("sort_order");
            "sort"
        }
        _ => "other",
    };
    stats.relations.insert(name);
    for child in children {
        visit_relation(child, depth + 1, stats);
    }
}
fn visit_expression(expression: &proto::Expression, stats: &mut Stats) {
    use proto::expression::ExprType as E;
    stats.expressions += 1;
    let name = match expression.expr_type.as_ref() {
        Some(E::Literal(lit)) => {
            visit_literal(lit, stats);
            "literal"
        }
        Some(E::UnresolvedAttribute(_)) => "unresolved_attribute",
        Some(E::UnresolvedFunction(f)) => {
            for e in &f.arguments {
                visit_expression(e, stats);
            }
            "unresolved_function"
        }
        Some(E::Alias(a)) => {
            if let Some(e) = a.expr.as_deref() {
                visit_expression(e, stats);
            }
            "alias"
        }
        Some(E::SortOrder(o)) => {
            if let Some(e) = o.child.as_deref() {
                visit_expression(e, stats);
            }
            "sort_order"
        }
        Some(E::Window(w)) => {
            if let Some(e) = w.window_function.as_deref() {
                visit_expression(e, stats);
            }
            for e in &w.partition_spec {
                visit_expression(e, stats);
            }
            for o in &w.order_spec {
                if let Some(e) = o.child.as_deref() {
                    visit_expression(e, stats);
                }
            }
            stats.expression_tags.insert("sort_order");
            "window"
        }
        _ => "other",
    };
    stats.expression_tags.insert(name);
}
fn visit_literal(literal: &proto::expression::Literal, stats: &mut Stats) {
    use proto::expression::literal::LiteralType as L;
    let name = match literal.literal_type.as_ref() {
        Some(L::Null(_)) => "null",
        Some(L::Boolean(_)) => "boolean",
        Some(L::Byte(_)) => "byte",
        Some(L::Short(_)) => "short",
        Some(L::Integer(_)) => "integer",
        Some(L::Long(_)) => "long",
        Some(L::Float(_)) => "float",
        Some(L::Double(_)) => "double",
        Some(L::Decimal(_)) => "decimal",
        Some(L::String(_)) => "string",
        Some(L::Binary(_)) => "binary",
        Some(L::Date(_)) => "date",
        Some(L::Timestamp(_)) => "timestamp",
        Some(L::TimestampNtz(_)) => "timestamp_ntz",
        Some(L::Array(a)) => {
            for e in &a.elements {
                visit_literal(e, stats);
            }
            "array"
        }
        _ => "other",
    };
    stats.literal_tags.insert(name);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    #[test]
    fn typed_literals_preserve_unmodified_pinned_client_output_and_array_element_type() {
        let case = typed_literals();
        let raw = raw_typed_literals();
        assert_eq!(case.relation.encode_to_vec(), raw.relation.encode_to_vec());
        assert_eq!(case.metadata, raw.metadata);
        assert_eq!(
            case.metadata["details"]["literal_data_type_fields"],
            "official_client_unmodified"
        );
        assert!(case.metadata.get("rust_api_gap_ids").is_none());
        let Some(proto::relation::RelType::Project(project)) = case.relation.rel_type else {
            panic!("typed literal projection");
        };
        let Some(proto::data_type::Kind::Struct(schema)) = case.expected_schema.kind else {
            panic!("typed literal schema");
        };
        assert_eq!(project.expressions.len(), schema.fields.len());
        let mut arrays = 0;
        for (expression, field) in project.expressions.into_iter().zip(schema.fields) {
            let Some(proto::expression::ExprType::Alias(alias)) = expression.expr_type else {
                panic!("typed literal alias");
            };
            let Some(proto::expression::ExprType::Literal(literal)) =
                alias.expr.and_then(|expression| expression.expr_type)
            else {
                panic!("typed literal value");
            };
            if let Some(root_type) = literal.data_type.as_ref() {
                assert_eq!(Some(root_type), field.data_type.as_ref());
            }
            if let Some(proto::expression::literal::LiteralType::Array(array)) =
                literal.literal_type
            {
                arrays += 1;
                let Some(proto::data_type::Kind::Array(data_type)) =
                    field.data_type.and_then(|data_type| data_type.kind)
                else {
                    panic!("declared array schema");
                };
                #[allow(deprecated)]
                {
                    if let Some(element_type) = array.element_type.as_ref() {
                        assert_eq!(Some(element_type), data_type.element_type.as_deref());
                    }
                    assert!(literal.data_type.is_some() || array.element_type.is_some());
                }
                assert_eq!(
                    data_type.element_type.as_deref(),
                    Some(&SparkType::Integer.to_proto())
                );
                assert!(data_type.contains_null);
                assert_eq!(array.elements.len(), 3);
                assert!(matches!(
                    array.elements[1].literal_type,
                    Some(proto::expression::literal::LiteralType::Null(_))
                ));
            }
        }
        assert_eq!(arrays, 1);
    }

    #[test]
    fn map_payload_uses_arrow_java_map_child_names_even_for_zero_rows() {
        for rows in [0, 1, 8] {
            let batch = make_batch("map", rows).unwrap();
            let ArrowType::Map(entries, _) = batch.column(0).data_type() else {
                panic!("Arrow map payload");
            };
            assert_eq!(entries.name(), "entries");
            let ArrowType::Struct(fields) = entries.data_type() else {
                panic!("Arrow map entry struct");
            };
            assert_eq!(fields[0].name(), "key");
            assert_eq!(fields[1].name(), "value");
            assert!(!fields[0].is_nullable());
            assert!(fields[1].is_nullable());
            batch.column(0).to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn corpus_is_deterministic_and_round_trips_the_real_request_envelope() {
        let first = build("smoke").unwrap();
        let second = build("smoke").unwrap();
        assert_eq!(first.len(), second.len());
        let mut names = BTreeSet::new();
        for (left, right) in first.iter().zip(&second) {
            assert!(names.insert(left.name.clone()));
            assert_eq!(
                left.relation.encode_to_vec(),
                right.relation.encode_to_vec()
            );
            assert_eq!(left.metadata, right.metadata);
            let request = proto::AnalyzePlanRequest {
                analyze: Some(proto::analyze_plan_request::Analyze::Schema(
                    proto::analyze_plan_request::Schema {
                        plan: Some(proto::Plan {
                            op_type: Some(proto::plan::OpType::Root(left.relation.clone())),
                        }),
                    },
                )),
                ..Default::default()
            };
            assert_eq!(
                proto::AnalyzePlanRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
                request
            );
            assert_eq!(
                left.metadata["encoded_relation_bytes"].as_u64().unwrap() as usize,
                left.relation.encoded_len()
            );
        }
    }

    #[test]
    fn arrow_cases_have_decodable_typed_rows_and_truthful_size_metadata() {
        for family in [
            "primitive",
            "variable",
            "decimal_temporal",
            "nested",
            "map",
            "null_heavy",
        ] {
            for target in [0, 4096, 65536] {
                let case = local_case(family, target).unwrap();
                let proto::relation::RelType::LocalRelation(local) =
                    case.relation.rel_type.unwrap()
                else {
                    panic!("local relation");
                };
                let bytes = local.data.unwrap();
                assert_eq!(
                    bytes.len(),
                    case.metadata["details"]["arrow_ipc_bytes"]
                        .as_u64()
                        .unwrap() as usize
                );
                assert!(
                    target == 0 || bytes.len() <= target,
                    "{family} {target} {}",
                    bytes.len()
                );
                let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
                assert_eq!(
                    arrow_schema_to_spark(reader.schema().as_ref())
                        .unwrap()
                        .to_proto(),
                    case.expected_schema
                );
                let batch = reader.next().unwrap().unwrap();
                assert_eq!(
                    batch.num_rows(),
                    case.metadata["details"]["input_rows"].as_u64().unwrap() as usize
                );
                assert!(reader.next().is_none());
                for column in batch.columns() {
                    column.to_data().validate_full().unwrap();
                }
            }
        }
    }

    #[test]
    fn stress_chain_and_union_fit_protobuf_decoder_recursion_limit() {
        // Reproduce the largest deep shapes without allocating the stress data corpus.
        let mut chain = plan::range(0, 1000, 1);
        for _ in 0..20 {
            chain = plan::project(chain, [col("id")]);
        }
        let mut union = plan::range(0, 1000, 1);
        for _ in 1..20 {
            union = plan::set_operation(
                union,
                plan::range(0, 1000, 1),
                SetOpType::Union,
                true,
                false,
                false,
            );
        }
        for plan in [chain, union] {
            let request = proto::AnalyzePlanRequest {
                analyze: Some(proto::analyze_plan_request::Analyze::Schema(
                    proto::analyze_plan_request::Schema {
                        plan: Some(proto::Plan {
                            op_type: Some(proto::plan::OpType::Root(plan.to_proto())),
                        }),
                    },
                )),
                ..Default::default()
            };
            proto::AnalyzePlanRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        }
    }

    #[test]
    fn invalid_profile_is_rejected() {
        assert!(build("unknown").is_err());
    }

    #[test]
    fn exported_manifest_and_binary_payloads_match_the_prebuilt_corpus() {
        struct Temporary(std::path::PathBuf);
        impl Drop for Temporary {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let directory = std::env::temp_dir().join(format!(
            "ferrum-corpus-export-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        let temporary = Temporary(directory);
        let cases = build("smoke").unwrap();
        let manifest = export(&cases, &temporary.0).unwrap();
        let persisted: Value =
            serde_json::from_slice(&fs::read(temporary.0.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(manifest, persisted);
        assert_eq!(manifest["cases"].as_array().unwrap().len(), cases.len());
        assert_eq!(manifest["payloads"].as_array().unwrap().len(), 13);
        for payload in manifest["payloads"].as_array().unwrap() {
            let schema = proto::DataType::decode(
                fs::read(temporary.0.join(payload["schema_file"].as_str().unwrap()))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let bytes =
                fs::read(temporary.0.join(payload["arrow_file"].as_str().unwrap())).unwrap();
            let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
            assert_eq!(
                schema,
                arrow_schema_to_spark(reader.schema().as_ref())
                    .unwrap()
                    .to_proto()
            );
            assert_eq!(
                reader.next().unwrap().unwrap().num_rows(),
                payload["rows"].as_u64().unwrap() as usize
            );
            assert!(reader.next().is_none());
        }
        // A second export cannot silently replace an immutable benchmark input.
        assert!(export(&cases, &temporary.0).is_err());
    }
}
