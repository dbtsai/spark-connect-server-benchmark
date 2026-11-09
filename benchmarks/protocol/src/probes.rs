//! Untimed protocol correctness and capability probes.
//!
//! Planning probes never call ExecutePlan, upload artifacts, mutate persistence,
//! or evaluate a SQL command. Fixture probes replay prebuilt Arrow responses.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::{Component, Path};
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use prost::Message;
use proto::spark_connect_service_client::SparkConnectServiceClient;
use serde_json::{json, Value};
use spark_connect_proto as proto;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};
use uuid::Uuid;

pub const ROUTES: [&str; 12] = [
    "ExecutePlan",
    "AnalyzePlan",
    "Config",
    "AddArtifacts",
    "ArtifactStatus",
    "Interrupt",
    "ReattachExecute",
    "ReleaseExecute",
    "ReleaseSession",
    "FetchErrorDetails",
    "CloneSession",
    "GetStatus",
];
const ANALYSES: [&str; 14] = [
    "schema",
    "explain",
    "tree_string",
    "is_local",
    "is_streaming",
    "input_files",
    "spark_version",
    "ddl_parse",
    "same_semantics",
    "semantic_hash",
    "persist",
    "unpersist",
    "get_storage_level",
    "json_to_ddl",
];
const MODIFIABLE_CONFIG: &str = "spark.sql.session.timeZone";

#[derive(Debug)]
enum Failure {
    Rpc(Status),
    Invalid(String),
}
impl From<Status> for Failure {
    fn from(value: Status) -> Self {
        Self::Rpc(value)
    }
}
impl From<String> for Failure {
    fn from(value: String) -> Self {
        Self::Invalid(value)
    }
}
type Checked<T = Value> = Result<T, Failure>;

fn require(condition: bool, message: &str) -> Checked<()> {
    if condition {
        Ok(())
    } else {
        Err(Failure::Invalid(message.into()))
    }
}
fn request<T>(value: T, timeout: Duration) -> Request<T> {
    let mut request = Request::new(value);
    request.set_timeout(timeout);
    request
}
fn plan() -> proto::Plan {
    proto::Plan {
        op_type: Some(proto::plan::OpType::Root(
            spark_connect::plan::range(0, 1000, 1).to_proto(),
        )),
    }
}
fn relation() -> proto::Relation {
    spark_connect::plan::range(0, 1000, 1).to_proto()
}

#[derive(Clone)]
struct Session {
    id: String,
    user: proto::UserContext,
    observed: Option<String>,
}
impl Session {
    fn observe(&mut self, id: &str, server: &str) -> Checked<()> {
        require(id == self.id, "response session identity changed")?;
        require(!server.is_empty(), "missing server-side session identity")?;
        if let Some(previous) = &self.observed {
            require(previous == server, "server-side identity changed")?;
        }
        self.observed = Some(server.into());
        Ok(())
    }
}

struct Prober {
    client: SparkConnectServiceClient<Channel>,
    timeout: Duration,
    records: Vec<Value>,
    sessions: Vec<Session>,
}
impl Prober {
    fn session(&mut self) -> Session {
        let session = Session {
            id: Uuid::new_v4().to_string(),
            user: proto::UserContext {
                user_id: "rust-protocol-probes".into(),
                ..Default::default()
            },
            observed: None,
        };
        self.sessions.push(session.clone());
        session
    }
    fn record(&mut self, route: &str, variant: &str, unsupported_allowed: bool, outcome: Checked) {
        let value = match outcome {
            Ok(details) => {
                json!({"route":route,"variant":variant,"status":details.get("actual_status").and_then(Value::as_str).unwrap_or("OK"),
                "passed":true,"supported":true,"tested":true,"details":details})
            }
            Err(Failure::Rpc(status))
                if status.code() == Code::Unimplemented && unsupported_allowed =>
            {
                json!({"route":route,"variant":variant,"status":"UNIMPLEMENTED","passed":true,
                    "supported":false,"tested":true,"details":status.message(),"capability_only":true})
            }
            Err(Failure::Rpc(status)) => {
                json!({"route":route,"variant":variant,"status":format!("{:?}",status.code()),
                "passed":false,"supported":status.code()!=Code::Unimplemented,"tested":true,"details":status.message()})
            }
            Err(Failure::Invalid(details)) => {
                json!({"route":route,"variant":variant,"status":"VALIDATION_FAILED",
                "passed":false,"supported":true,"tested":true,"details":details})
            }
        };
        self.records.push(value);
    }
    fn excluded(&mut self, route: &str, variant: &str, reason: &str) {
        self.records.push(
            json!({"route":route,"variant":variant,"tested":false,"status":"NOT_TESTED",
            "supported":null,"passed":true,"reason":reason}),
        );
    }
    async fn config(
        &mut self,
        session: &mut Session,
        op: proto::config_request::operation::OpType,
    ) -> Checked<proto::ConfigResponse> {
        let response = self
            .client
            .config(request(
                proto::ConfigRequest {
                    session_id: session.id.clone(),
                    user_context: Some(session.user.clone()),
                    client_observed_server_side_session_id: session.observed.clone(),
                    operation: Some(proto::config_request::Operation { op_type: Some(op) }),
                    ..Default::default()
                },
                self.timeout,
            ))
            .await?
            .into_inner();
        session.observe(&response.session_id, &response.server_side_session_id)?;
        Ok(response)
    }
    async fn release(&mut self, session: &mut Session) -> Checked {
        let response = self
            .client
            .release_session(request(
                proto::ReleaseSessionRequest {
                    session_id: session.id.clone(),
                    user_context: Some(session.user.clone()),
                    ..Default::default()
                },
                self.timeout,
            ))
            .await?
            .into_inner();
        session.observe(&response.session_id, &response.server_side_session_id)?;
        Ok(json!({"identity_valid":true}))
    }
    async fn release_operation(
        &mut self,
        session: &mut Session,
        operation: &str,
        release: proto::release_execute_request::Release,
    ) -> Checked {
        let response = self
            .client
            .release_execute(request(
                proto::ReleaseExecuteRequest {
                    session_id: session.id.clone(),
                    user_context: Some(session.user.clone()),
                    operation_id: operation.into(),
                    client_observed_server_side_session_id: session.observed.clone(),
                    release: Some(release),
                    ..Default::default()
                },
                self.timeout,
            ))
            .await?
            .into_inner();
        session.observe(&response.session_id, &response.server_side_session_id)?;
        require(
            response.operation_id.as_deref() == Some(operation),
            "release operation identity missing or changed",
        )?;
        Ok(json!({"operation_id":operation}))
    }
    async fn reattach(
        &mut self,
        session: &Session,
        operation: &str,
        last: Option<String>,
    ) -> Checked<Vec<proto::ExecutePlanResponse>> {
        let stream = self
            .client
            .reattach_execute(request(
                proto::ReattachExecuteRequest {
                    session_id: session.id.clone(),
                    user_context: Some(session.user.clone()),
                    operation_id: operation.into(),
                    client_observed_server_side_session_id: session.observed.clone(),
                    last_response_id: last,
                    ..Default::default()
                },
                self.timeout,
            ))
            .await?
            .into_inner();
        collect(stream, self.timeout).await
    }
}

/// Run untimed probes and return explicit per-route capability/correctness records.
pub async fn run(
    endpoint: &str,
    profile: &str,
    payload_dir: &Path,
    timeout: Duration,
) -> Result<Value, String> {
    if !matches!(profile, "fixture" | "planning") || timeout.is_zero() {
        return Err("probes require fixture/planning profile and positive timeout".into());
    }
    let url = if let Some(address) = endpoint.strip_prefix("sc://") {
        format!("http://{address}")
    } else if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    };
    let channel = Endpoint::from_shared(url)
        .map_err(|e| e.to_string())?
        .connect_timeout(timeout)
        .timeout(timeout)
        .connect()
        .await
        .map_err(|e| e.to_string())?;
    let mut prober = Prober {
        client: SparkConnectServiceClient::new(channel)
            .max_decoding_message_size(64 * 1024 * 1024)
            .max_encoding_message_size(64 * 1024 * 1024),
        timeout,
        records: vec![],
        sessions: vec![],
    };
    let payloads = if profile == "fixture" {
        load_payloads(payload_dir)?
    } else {
        vec![]
    };
    let phase = async {
        let mut session = prober.session();
        config_probes(&mut prober, &mut session).await;
        analyze_probes(&mut prober, &mut session, profile).await;
        if profile == "fixture" {
            let result = execute_probes(&mut prober, &mut session, &payloads).await;
            if let Err(error) = result {
                prober.record("ExecutePlan", "stream_lifecycle_group", false, Err(error));
            }
            artifact_probes(&mut prober, &mut session).await;
            clone_probe(&mut prober, &mut session).await;
            for selector in ["operation_id", "tag", "all"] {
                let result = interrupt_probe(&mut prober, &mut session, selector).await;
                prober.record("Interrupt", selector, false, result);
            }
            error_probes(&mut prober, &mut session).await;
            let released = prober.release(&mut session).await;
            prober.record("ReleaseSession", "release", false, released);
            let result = prober
                .config(
                    &mut session,
                    proto::config_request::operation::OpType::GetOption(
                        proto::config_request::GetOption {
                            keys: vec!["spark.sql.session.timeZone".into()],
                        },
                    ),
                )
                .await;
            prober.record(
                "ReleaseSession",
                "tombstone",
                false,
                expect_status(result, Code::FailedPrecondition),
            );
        } else {
            for route in ROUTES {
                if !matches!(route, "AnalyzePlan" | "Config" | "ReleaseSession") {
                    prober.excluded(route,"planning_profile","excluded to preserve no-execution/no-artifact/no-operation planning profile");
                }
            }
        }
    };
    if tokio::time::timeout(
        timeout.saturating_mul(128).min(Duration::from_secs(600)),
        phase,
    )
    .await
    .is_err()
    {
        prober.record(
            "ProbePhase",
            "deadline",
            false,
            Err(Failure::Invalid(
                "entire probe phase exceeded its bounded deadline".into(),
            )),
        );
    }
    // Cleanup occurs even if the phase is cancelled or a correctness check fails.
    for mut session in prober.sessions.clone() {
        let result = prober.release(&mut session).await;
        prober.record("ReleaseSession", "cleanup", false, result);
    }
    let inventory=ROUTES.iter().map(|route|{
        let records=prober.records.iter().filter(|record|record["route"]==*route&&record["tested"]==true).collect::<Vec<_>>();
        json!({"route":route,"tested":!records.is_empty(),"probe_count":records.len(),
            "supported":records.iter().any(|record|record["supported"]==true),
            "not_tested_reason":if records.is_empty(){Some(if profile=="planning" {"execution-free planning profile excludes this route"} else {"probe phase did not reach this route"})}else{None}})
    }).collect::<Vec<_>>();
    let passed = prober.records.iter().all(|record| record["passed"] == true);
    Ok(
        json!({"profile":profile,"passed":passed,"query_execution_requested":false,
        "fixture_execute_semantics":if profile=="fixture" {Some("prebuilt payload replay only")} else {None},
        "routes_total":12,"routes_tested":inventory.iter().filter(|route|route["tested"]==true).count(),
        "analyze_variants_total":14,"config_variants_total":7,"inventory":inventory,"records":prober.records,
        "timing_semantics":"untimed correctness/capability probes; UNIMPLEMENTED is never throughput success"}),
    )
}

fn pairs(response: &proto::ConfigResponse) -> BTreeMap<String, Option<String>> {
    response
        .pairs
        .iter()
        .map(|pair| (pair.key.clone(), pair.value.clone()))
        .collect()
}
fn validate_config_pairs(
    variant: &str,
    actual: &BTreeMap<String, Option<String>>,
    key: &str,
    prefix: &str,
    value: &str,
) -> Checked<()> {
    match variant {
        "set" | "unset" => require(actual.is_empty(), "config write returned unexpected values"),
        "get" => require(
            *actual == BTreeMap::from([(key.into(), Some(value.into()))]),
            "Config.Get returned wrong value",
        ),
        "get_option" => require(
            actual.get(key) == Some(&Some(value.into()))
                && actual.get(&format!("{key}.missing")) == Some(&None),
            "GetOption missing/null semantics changed",
        ),
        "get_with_default" => require(
            *actual == BTreeMap::from([(format!("{key}.missing"), Some("fallback".into()))]),
            "config fallback changed",
        ),
        "get_all" => {
            // Spark filters on the original key, then removes the requested
            // prefix from each returned key (SparkConnectConfigHandler).
            let suffix = key
                .strip_prefix(prefix)
                .ok_or_else(|| Failure::Invalid("probe key lacks its namespace prefix".into()))?;
            require(
                *actual == BTreeMap::from([(suffix.into(), Some(value.into()))]),
                "GetAll prefix stripping or value changed",
            )
        }
        "is_modifiable" => require(
            *actual == BTreeMap::from([(MODIFIABLE_CONFIG.into(), Some("true".into()))]),
            "registered runtime config is unexpectedly unmodifiable",
        ),
        _ => unreachable!(),
    }
}
async fn config_probes(prober: &mut Prober, session: &mut Session) {
    use proto::config_request::{self as c, operation::OpType as O};
    let prefix = format!("bench.protocol.{}.", Uuid::new_v4().simple());
    let key = format!("{prefix}value");
    let value = "before π😀".to_owned();
    let operations = vec![
        (
            "set",
            O::Set(c::Set {
                pairs: vec![proto::KeyValue {
                    key: key.clone(),
                    value: Some(value.clone()),
                }],
                ..Default::default()
            }),
        ),
        (
            "get",
            O::Get(c::Get {
                keys: vec![key.clone()],
            }),
        ),
        (
            "get_option",
            O::GetOption(c::GetOption {
                keys: vec![key.clone(), format!("{key}.missing")],
            }),
        ),
        (
            "get_with_default",
            O::GetWithDefault(c::GetWithDefault {
                pairs: vec![proto::KeyValue {
                    key: format!("{key}.missing"),
                    value: Some("fallback".into()),
                }],
            }),
        ),
        (
            "get_all",
            O::GetAll(c::GetAll {
                prefix: Some(prefix.clone()),
            }),
        ),
        (
            "is_modifiable",
            O::IsModifiable(c::IsModifiable {
                // An arbitrary custom key can be set in Spark but is not a
                // registered SQLConf entry, so Spark reports it unmodifiable.
                keys: vec![MODIFIABLE_CONFIG.into()],
            }),
        ),
        (
            "unset",
            O::Unset(c::Unset {
                keys: vec![key.clone()],
            }),
        ),
    ];
    for (variant, operation) in operations {
        let result = async {
            let response = prober.config(session, operation).await?;
            let actual = pairs(&response);
            require(
                actual.len() == response.pairs.len(),
                "duplicate config response keys",
            )?;
            validate_config_pairs(variant, &actual, &key, &prefix, &value)?;
            Ok(json!({"pairs":actual,"identity_valid":true}))
        }
        .await;
        prober.record("Config", variant, true, result);
    }
    let result = async {
        let response = prober
            .config(
                session,
                O::GetOption(c::GetOption {
                    keys: vec![key.clone()],
                }),
            )
            .await?;
        require(
            pairs(&response) == BTreeMap::from([(key, None)]),
            "Unset did not remove custom config",
        )?;
        Ok(json!({"removed":true}))
    }
    .await;
    prober.record("Config", "unset_verified", true, result);
}

fn analysis(variant: &str) -> proto::analyze_plan_request::Analyze {
    use proto::analyze_plan_request::{self as a, Analyze as A};
    match variant {
        "schema"=>A::Schema(a::Schema {plan:Some(plan())}),
        "explain"=>A::Explain(a::Explain {plan:Some(plan()),explain_mode:1}),
        "tree_string"=>A::TreeString(a::TreeString {plan:Some(plan()),level:None}),
        "is_local"=>A::IsLocal(a::IsLocal {plan:Some(plan())}),
        "is_streaming"=>A::IsStreaming(a::IsStreaming {plan:Some(plan())}),
        "input_files"=>A::InputFiles(a::InputFiles {plan:Some(plan())}),
        "spark_version"=>A::SparkVersion(a::SparkVersion {}),
        "ddl_parse"=>A::DdlParse(a::DdlParse {ddl_string:"id BIGINT".into()}),
        "same_semantics"=>A::SameSemantics(a::SameSemantics {target_plan:Some(plan()),other_plan:Some(plan())}),
        "semantic_hash"=>A::SemanticHash(a::SemanticHash {plan:Some(plan())}),
        "persist"=>A::Persist(a::Persist {relation:Some(relation()),storage_level:None}),
        "unpersist"=>A::Unpersist(a::Unpersist {relation:Some(relation()),blocking:Some(false)}),
        "get_storage_level"=>A::GetStorageLevel(a::GetStorageLevel {relation:Some(relation())}),
        "json_to_ddl"=>A::JsonToDdl(a::JsonToDdl {json_string:r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#.into()}),
        _=>unreachable!(),
    }
}
async fn analyze_probes(prober: &mut Prober, session: &mut Session, profile: &str) {
    for variant in ANALYSES {
        if profile == "planning" && matches!(variant, "persist" | "unpersist") {
            prober.excluded(
                "AnalyzePlan",
                variant,
                "persistence mutations are excluded from the execution-free planning profile",
            );
            continue;
        }
        let result = async {
            let response = prober
                .client
                .analyze_plan(request(
                    proto::AnalyzePlanRequest {
                        session_id: session.id.clone(),
                        user_context: Some(session.user.clone()),
                        client_observed_server_side_session_id: session.observed.clone(),
                        analyze: Some(analysis(variant)),
                        ..Default::default()
                    },
                    prober.timeout,
                ))
                .await?
                .into_inner();
            session.observe(&response.session_id, &response.server_side_session_id)?;
            validate_analysis(variant, response.result, profile)?;
            Ok(json!({"matching_response_variant":true,"identity_valid":true}))
        }
        .await;
        let unsupported = profile == "planning"
            || !matches!(
                variant,
                "schema" | "is_local" | "is_streaming" | "spark_version"
            );
        prober.record("AnalyzePlan", variant, unsupported, result);
    }
}
fn validate_analysis(
    variant: &str,
    result: Option<proto::analyze_plan_response::Result>,
    profile: &str,
) -> Checked<()> {
    use proto::analyze_plan_response::Result as R;
    match (variant, result) {
        ("schema", Some(R::Schema(s))) => validate_id_schema(s.schema.as_ref())?,
        ("is_local", Some(R::IsLocal(s))) => require(
            s.is_local == (profile == "fixture"),
            "unexpected IsLocal result for range",
        )?,
        ("is_streaming", Some(R::IsStreaming(s))) => {
            require(!s.is_streaming, "range is unexpectedly streaming")?
        }
        ("spark_version", Some(R::SparkVersion(s))) => {
            require(!s.version.is_empty(), "empty Spark version")?
        }
        ("explain", Some(R::Explain(s))) => require(
            s.explain_string.to_lowercase().contains("range"),
            "explain did not contain range",
        )?,
        ("tree_string", Some(R::TreeString(s))) => require(
            s.tree_string.contains("id") && s.tree_string.contains("long"),
            "tree string is missing id/long",
        )?,
        ("input_files", Some(R::InputFiles(s))) => {
            require(s.files.is_empty(), "in-memory range has input files")?
        }
        ("ddl_parse", Some(R::DdlParse(s))) => validate_id_schema(s.parsed.as_ref())?,
        ("same_semantics", Some(R::SameSemantics(s))) => {
            require(s.result, "identical range plans are not semantically equal")?
        }
        ("semantic_hash", Some(R::SemanticHash(_))) => {}
        ("get_storage_level", Some(R::GetStorageLevel(s))) => {
            require(s.storage_level.is_some(), "missing storage level")?
        }
        ("json_to_ddl", Some(R::JsonToDdl(s))) => require(
            s.ddl_string.to_lowercase().contains("id")
                && s.ddl_string.to_lowercase().contains("bigint"),
            "JSON-to-DDL result changed",
        )?,
        // Synthetic fixtures explicitly reject persistence/plan-resolution work.
        ("persist", Some(R::Persist(_))) | ("unpersist", Some(R::Unpersist(_))) => {
            return Err(Failure::Invalid(
                "fixture unexpectedly accepted a persistence mutation".into(),
            ))
        }
        _ => {
            return Err(Failure::Invalid(format!(
                "analysis {variant} returned the wrong or missing oneof"
            )))
        }
    }
    Ok(())
}
fn validate_id_schema(schema: Option<&proto::DataType>) -> Checked<()> {
    let Some(proto::DataType {
        kind: Some(proto::data_type::Kind::Struct(schema)),
        ..
    }) = schema
    else {
        return Err(Failure::Invalid("schema is not a struct".into()));
    };
    require(
        schema.fields.len() == 1
            && schema.fields[0].name == "id"
            && matches!(
                schema.fields[0]
                    .data_type
                    .as_ref()
                    .and_then(|t| t.kind.as_ref()),
                Some(proto::data_type::Kind::Long(_))
            ),
        "schema is not id:long",
    )
}

struct Payload {
    key: String,
    schema: proto::DataType,
    bytes: Vec<u8>,
    rows: i64,
}
fn load_payloads(directory: &Path) -> Result<Vec<Payload>, String> {
    let manifest: Value = serde_json::from_slice(
        &std::fs::read(directory.join("manifest.json")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let mut payloads = vec![];
    for entry in manifest["payloads"]
        .as_array()
        .ok_or("payload manifest has no payloads")?
    {
        let read = |key: &str| -> Result<Vec<u8>, String> {
            let name = entry[key].as_str().ok_or("payload file missing")?;
            if Path::new(name)
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
            {
                return Err("payload path escapes directory".into());
            }
            std::fs::read(directory.join(name)).map_err(|e| e.to_string())
        };
        let payload = Payload {
            key: entry["key"].as_str().ok_or("payload key missing")?.into(),
            schema: proto::DataType::decode(read("schema_file")?.as_slice())
                .map_err(|e| e.to_string())?,
            bytes: read("arrow_file")?,
            rows: entry["rows"]
                .as_i64()
                .filter(|rows| *rows >= 0)
                .ok_or("invalid payload rows")?,
        };
        let rows = StreamReader::try_new(Cursor::new(&payload.bytes), None)
            .map_err(|e| e.to_string())?
            .try_fold(0_i64, |rows, batch| {
                batch.map(|batch| rows + batch.num_rows() as i64)
            })
            .map_err(|e| e.to_string())?;
        if rows != payload.rows {
            return Err("manifest Arrow row count differs".into());
        }
        payloads.push(payload);
    }
    payloads.sort_by(|a, b| a.key.cmp(&b.key));
    if !payloads.iter().any(|p| p.key == "range") {
        return Err("payload manifest needs range fallback".into());
    }
    Ok(payloads)
}
async fn collect(
    mut stream: tonic::Streaming<proto::ExecutePlanResponse>,
    timeout: Duration,
) -> Checked<Vec<proto::ExecutePlanResponse>> {
    let mut messages = vec![];
    while let Some(message) = tokio::time::timeout(timeout, stream.message())
        .await
        .map_err(|_| Failure::Invalid("stream message timeout".into()))??
    {
        require(
            messages.len() < 128,
            "fixture stream exceeds bounded message count",
        )?;
        messages.push(message);
    }
    Ok(messages)
}
fn validate_stream(
    session: &mut Session,
    operation: &str,
    messages: &[proto::ExecutePlanResponse],
    payload: &Payload,
    batches: usize,
) -> Checked {
    require(
        messages.len() == batches + 2,
        "wrong number of schema/batch/completion messages",
    )?;
    let mut ids = BTreeSet::new();
    for message in messages {
        session.observe(&message.session_id, &message.server_side_session_id)?;
        require(
            message.operation_id == operation,
            "stream operation identity changed",
        )?;
        require(
            Uuid::parse_str(&message.response_id).is_ok() && ids.insert(&message.response_id),
            "missing/duplicate/invalid response id",
        )?;
    }
    require(
        messages[0].schema.as_ref() == Some(&payload.schema) && messages[0].response_type.is_none(),
        "schema header changed",
    )?;
    for (index, message) in messages[1..=batches].iter().enumerate() {
        let Some(proto::execute_plan_response::ResponseType::ArrowBatch(batch)) =
            &message.response_type
        else {
            return Err(Failure::Invalid("expected Arrow batch".into()));
        };
        require(
            batch.row_count == payload.rows
                && batch.start_offset == Some(index as i64 * payload.rows),
            "batch row count or offset changed",
        )?;
        require(
            batch.data.as_ref() == payload.bytes.as_slice(),
            "Arrow payload bytes changed",
        )?;
        require(
            crc32fast::hash(&batch.data) == crc32fast::hash(&payload.bytes),
            "Arrow CRC changed",
        )?;
        let rows = StreamReader::try_new(Cursor::new(batch.data.as_ref()), None)
            .map_err(|e| Failure::Invalid(e.to_string()))?
            .try_fold(0_i64, |rows, batch| {
                batch.map(|batch| rows + batch.num_rows() as i64)
            })
            .map_err(|e| Failure::Invalid(e.to_string()))?;
        require(
            rows == batch.row_count,
            "Arrow IPC rows differ from envelope",
        )?;
    }
    require(
        matches!(
            messages.last().and_then(|m| m.response_type.as_ref()),
            Some(proto::execute_plan_response::ResponseType::ResultComplete(
                _
            ))
        ),
        "missing final completion",
    )?;
    Ok(
        json!({"rows":payload.rows*batches as i64,"batches":batches,"ipc_bytes_per_batch":payload.bytes.len(),"crc32":crc32fast::hash(&payload.bytes),"ids_schema_offsets_values_verified":true}),
    )
}
fn execute_request(
    session: &Session,
    operation: &str,
    tags: Vec<String>,
) -> proto::ExecutePlanRequest {
    proto::ExecutePlanRequest {
        session_id: session.id.clone(),
        user_context: Some(session.user.clone()),
        client_observed_server_side_session_id: session.observed.clone(),
        operation_id: Some(operation.into()),
        plan: Some(plan()),
        tags,
        ..Default::default()
    }
}
async fn execute_probes(
    prober: &mut Prober,
    session: &mut Session,
    payloads: &[Payload],
) -> Checked<()> {
    for payload in payloads {
        let operation = Uuid::new_v4().to_string();
        let batches = if payload.key == "range" { 2 } else { 1 };
        let stream = prober
            .client
            .execute_plan(request(
                execute_request(
                    session,
                    &operation,
                    vec![
                        format!("bench.payload={}", payload.key),
                        format!("bench.batches={batches}"),
                    ],
                ),
                prober.timeout,
            ))
            .await?
            .into_inner();
        let messages = collect(stream, prober.timeout).await?;
        let result = validate_stream(session, &operation, &messages, payload, batches);
        let valid_stream = result.is_ok();
        prober.record(
            "ExecutePlan",
            &format!("payload_{}", payload.key),
            false,
            result,
        );
        // A failed validation is recorded above. Do not index a malformed or
        // empty stream while probing its cursor; still release the operation.
        if payload.key == "range" && valid_stream {
            let resumed = prober
                .reattach(session, &operation, Some(messages[0].response_id.clone()))
                .await?;
            let result = require(
                resumed == messages[1..],
                "reattach did not resume after last response",
            )
            .map(|_| json!({"replayed_messages":resumed.len()}));
            prober.record("ReattachExecute", "last_response_cursor", false, result);
            status_probe(prober, session, &operation, 4, "completed_operation").await;
            let result = prober
                .release_operation(
                    session,
                    &operation,
                    proto::release_execute_request::Release::ReleaseUntil(
                        proto::release_execute_request::ReleaseUntil {
                            response_id: messages[1].response_id.clone(),
                        },
                    ),
                )
                .await;
            prober.record("ReleaseExecute", "release_until", false, result);
            let rejected = prober.reattach(session, &operation, None).await;
            prober.record(
                "ReattachExecute",
                "released_prefix_rejected",
                false,
                expect_status(rejected, Code::NotFound),
            );
            let suffix = prober
                .reattach(session, &operation, Some(messages[1].response_id.clone()))
                .await?;
            prober.record(
                "ReattachExecute",
                "resume_after_released_prefix",
                false,
                require(
                    suffix == messages[2..],
                    "released-prefix cursor changed suffix",
                )
                .map(|_| json!({"suffix_messages":suffix.len()})),
            );
        }
        let result = prober
            .release_operation(
                session,
                &operation,
                proto::release_execute_request::Release::ReleaseAll(Default::default()),
            )
            .await;
        prober.record("ReleaseExecute", "release_all", false, result);
        if payload.key == "range" {
            let rejected = prober.reattach(session, &operation, None).await;
            prober.record(
                "ReattachExecute",
                "released_operation_rejected",
                false,
                expect_status(rejected, Code::NotFound),
            );
        }
    }
    Ok(())
}
async fn status_probe(
    prober: &mut Prober,
    session: &mut Session,
    operation: &str,
    state: i32,
    variant: &str,
) {
    let result = async {
        let response = prober
            .client
            .get_status(request(
                proto::GetStatusRequest {
                    session_id: session.id.clone(),
                    user_context: Some(session.user.clone()),
                    client_observed_server_side_session_id: session.observed.clone(),
                    operation_status: Some(proto::get_status_request::OperationStatusRequest {
                        operation_ids: vec![operation.into()],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                prober.timeout,
            ))
            .await?
            .into_inner();
        session.observe(&response.session_id, &response.server_side_session_id)?;
        require(
            response.operation_statuses.len() == 1
                && response.operation_statuses[0].operation_id == operation
                && response.operation_statuses[0].state == state,
            "GetStatus operation/state changed",
        )?;
        Ok(json!({"operation_state":state}))
    }
    .await;
    prober.record("GetStatus", variant, true, result);
}
async fn interrupt_probe(prober: &mut Prober, session: &mut Session, selector: &str) -> Checked {
    let operation = Uuid::new_v4().to_string();
    let tag = format!("probe.{}", Uuid::new_v4().simple());
    let mut stream = prober
        .client
        .execute_plan(request(
            execute_request(
                session,
                &operation,
                vec!["bench.hold=true".into(), tag.clone()],
            ),
            prober.timeout,
        ))
        .await?
        .into_inner();
    let header = tokio::time::timeout(prober.timeout, stream.message())
        .await
        .map_err(|_| Failure::Invalid("held operation header timeout".into()))??
        .ok_or_else(|| Failure::Invalid("held operation missing schema header".into()))?;
    session.observe(&header.session_id, &header.server_side_session_id)?;
    require(
        header.schema.is_some() && header.operation_id == operation,
        "held operation header invalid",
    )?;
    status_probe(prober, session, &operation, 2, "running_operation").await;
    let (kind, interrupt) = match selector {
        "operation_id" => (
            3,
            Some(proto::interrupt_request::Interrupt::OperationId(
                operation.clone(),
            )),
        ),
        "tag" => (
            2,
            Some(proto::interrupt_request::Interrupt::OperationTag(tag)),
        ),
        "all" => (1, None),
        _ => unreachable!(),
    };
    let response = prober
        .client
        .interrupt(request(
            proto::InterruptRequest {
                session_id: session.id.clone(),
                user_context: Some(session.user.clone()),
                client_observed_server_side_session_id: session.observed.clone(),
                interrupt_type: kind,
                interrupt,
                ..Default::default()
            },
            prober.timeout,
        ))
        .await?
        .into_inner();
    session.observe(&response.session_id, &response.server_side_session_id)?;
    require(
        response.interrupted_ids == vec![operation.clone()],
        "interrupt returned wrong operation ids",
    )?;
    let terminal = tokio::time::timeout(prober.timeout, stream.message())
        .await
        .map_err(|_| Failure::Invalid("interrupted operation did not terminate".into()))?;
    let result = expect_status(terminal, Code::Cancelled)?;
    status_probe(prober, session, &operation, 6, "cancelled_operation").await;
    let released = prober
        .release_operation(
            session,
            &operation,
            proto::release_execute_request::Release::ReleaseAll(Default::default()),
        )
        .await;
    prober.record("ReleaseExecute", "cancelled_operation", false, released);
    Ok(json!({"interrupted_ids":response.interrupted_ids,"terminal":result}))
}
fn expect_status<T>(result: Result<T, impl Into<Failure>>, expected: Code) -> Checked {
    match result.map_err(Into::into) {
        Err(Failure::Rpc(status)) if status.code() == expected => Ok(
            json!({"actual_status":format!("{expected:?}").to_uppercase(),"details":status.message()}),
        ),
        Err(other) => Err(other),
        Ok(_) => Err(Failure::Invalid(format!(
            "expected {expected:?}, call succeeded"
        ))),
    }
}

async fn artifact_probes(prober: &mut Prober, session: &mut Session) {
    use proto::add_artifacts_request::{self as a, Payload as P};
    let suffix = Uuid::new_v4().simple().to_string();
    let good = format!("files/{suffix}.bin");
    let bad = format!("files/{suffix}-bad.bin");
    let chunked = format!("files/{suffix}-chunked.bin");
    let chunk = |bytes: &[u8], valid: bool| a::ArtifactChunk {
        data: bytes.to_vec().into(),
        crc: i64::from(crc32fast::hash(bytes) ^ u32::from(!valid)),
    };
    let artifacts = vec![
        (
            "batched_crc",
            vec![P::Batch(a::Batch {
                artifacts: vec![
                    a::SingleChunkArtifact {
                        name: good.clone(),
                        data: Some(chunk(b"hello\0\xff", true)),
                    },
                    a::SingleChunkArtifact {
                        name: bad.clone(),
                        data: Some(chunk(b"bad crc", false)),
                    },
                ],
            })],
        ),
        (
            "chunked_crc",
            vec![
                P::BeginChunk(a::BeginChunkedArtifact {
                    name: chunked.clone(),
                    total_bytes: 6,
                    num_chunks: 2,
                    initial_chunk: Some(chunk(b"abc", true)),
                }),
                P::Chunk(chunk(b"def", true)),
            ],
        ),
    ];
    for (variant, payloads) in artifacts {
        let input = payloads
            .into_iter()
            .map(|payload| proto::AddArtifactsRequest {
                session_id: session.id.clone(),
                user_context: Some(session.user.clone()),
                client_observed_server_side_session_id: session.observed.clone(),
                payload: Some(payload),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let result = async {
            let response = prober
                .client
                .add_artifacts(request(tokio_stream::iter(input), prober.timeout))
                .await?
                .into_inner();
            session.observe(&response.session_id, &response.server_side_session_id)?;
            let actual = response
                .artifacts
                .iter()
                .map(|a| (a.name.clone(), a.is_crc_successful))
                .collect::<BTreeMap<_, _>>();
            let expected = if variant == "batched_crc" {
                BTreeMap::from([(good.clone(), true), (bad.clone(), false)])
            } else {
                BTreeMap::from([(chunked.clone(), true)])
            };
            require(actual == expected, "artifact CRC acknowledgements changed")?;
            Ok(json!({"artifact_crc_results":actual}))
        }
        .await;
        prober.record("AddArtifacts", variant, false, result);
    }
    let result = async {
        let missing = format!("files/{suffix}-missing.bin");
        let response = prober
            .client
            .artifact_status(request(
                proto::ArtifactStatusesRequest {
                    session_id: session.id.clone(),
                    user_context: Some(session.user.clone()),
                    client_observed_server_side_session_id: session.observed.clone(),
                    names: vec![good.clone(), bad.clone(), chunked.clone(), missing.clone()],
                    ..Default::default()
                },
                prober.timeout,
            ))
            .await?
            .into_inner();
        session.observe(&response.session_id, &response.server_side_session_id)?;
        let actual = response
            .statuses
            .into_iter()
            .map(|(name, status)| (name, status.exists))
            .collect::<BTreeMap<_, _>>();
        require(
            actual
                == BTreeMap::from([
                    (good, true),
                    (bad, false),
                    (chunked, true),
                    (missing, false),
                ]),
            "artifact status/failed-CRC discard changed",
        )?;
        Ok(json!({"statuses":actual,"failed_crc_discarded":true}))
    }
    .await;
    prober.record(
        "ArtifactStatus",
        "presence_and_failed_crc_discard",
        false,
        result,
    );
}
async fn clone_probe(prober: &mut Prober, session: &mut Session) {
    use proto::config_request::{self as c, operation::OpType as O};
    let key = format!("bench.protocol.clone.{}", Uuid::new_v4().simple());
    let result = async {
        prober
            .config(
                session,
                O::Set(c::Set {
                    pairs: vec![proto::KeyValue {
                        key: key.clone(),
                        value: Some("parent".into()),
                    }],
                    ..Default::default()
                }),
            )
            .await?;
        let mut cloned = prober.session();
        let response = prober
            .client
            .clone_session(request(
                proto::CloneSessionRequest {
                    session_id: session.id.clone(),
                    user_context: Some(session.user.clone()),
                    client_observed_server_side_session_id: session.observed.clone(),
                    new_session_id: Some(cloned.id.clone()),
                    ..Default::default()
                },
                prober.timeout,
            ))
            .await?
            .into_inner();
        session.observe(&response.session_id, &response.server_side_session_id)?;
        cloned.observe(
            &response.new_session_id,
            &response.new_server_side_session_id,
        )?;
        require(cloned.id != session.id, "clone reused parent identity")?;
        let actual = prober
            .config(
                &mut cloned,
                O::Get(c::Get {
                    keys: vec![key.clone()],
                }),
            )
            .await?;
        require(
            pairs(&actual) == BTreeMap::from([(key.clone(), Some("parent".into()))]),
            "clone did not inherit configuration",
        )?;
        prober
            .config(
                &mut cloned,
                O::Set(c::Set {
                    pairs: vec![proto::KeyValue {
                        key: key.clone(),
                        value: Some("child".into()),
                    }],
                    ..Default::default()
                }),
            )
            .await?;
        let parent = prober
            .config(
                session,
                O::Get(c::Get {
                    keys: vec![key.clone()],
                }),
            )
            .await?;
        require(
            pairs(&parent) == BTreeMap::from([(key, Some("parent".into()))]),
            "cloned configuration changed parent",
        )?;
        Ok(json!({"inherited_config":true,"isolated_mutation":true,"new_session_id":cloned.id}))
    }
    .await;
    prober.record("CloneSession", "configuration_copy_isolation", true, result);
}
async fn error_probes(prober: &mut Prober, session: &mut Session) {
    for error_id in ["missing-fixture-error", "fixture-error"] {
        let result=async {
            let response=prober.client.fetch_error_details(request(proto::FetchErrorDetailsRequest {session_id:session.id.clone(),user_context:Some(session.user.clone()),client_observed_server_side_session_id:session.observed.clone(),error_id:error_id.into(),..Default::default()},prober.timeout)).await?.into_inner();
            session.observe(&response.session_id,&response.server_side_session_id)?;
            if error_id=="fixture-error" {
                require(response.root_error_idx==Some(0)&&response.errors.len()==1,"missing structured root error")?;
                let error=&response.errors[0];
                require(error.error_type_hierarchy==vec!["ProtocolFixtureError","java.lang.RuntimeException"]&&error.message=="synthetic protocol fixture error"&&error.stack_trace.len()==1,"structured error hierarchy/message/stack changed")?;
                let frame=&error.stack_trace[0];
                require(frame.declaring_class=="ProtocolFixture"&&frame.method_name=="decode"&&frame.file_name.as_deref()==Some("fixture")&&frame.line_number==1,"structured error stack changed")?;
                let details=error.spark_throwable.as_ref().ok_or_else(||Failure::Invalid("missing SparkThrowable".into()))?;
                require(details.error_class.as_deref()==Some("FIXTURE_ERROR")&&details.sql_state.as_deref()==Some("XX000")&&details.message_parameters.get("kind").map(String::as_str)==Some("synthetic"),"structured SparkThrowable changed")?;
            } else {require(response.root_error_idx.is_none()&&response.errors.is_empty(),"missing error unexpectedly found")?;}
            Ok(json!({"error_id":error_id,"errors":response.errors.len(),"root_error_idx":response.root_error_idx}))
        }.await;
        prober.record("FetchErrorDetails", error_id, false, result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_validation_matches_spark_prefix_stripping_and_registered_modifiability() {
        let prefix = "bench.protocol.unique.";
        let key = "bench.protocol.unique.value";
        let value = "before π😀";
        let stripped = BTreeMap::from([("value".into(), Some(value.into()))]);
        assert!(validate_config_pairs("get_all", &stripped, key, prefix, value).is_ok());
        let unstripped = BTreeMap::from([(key.into(), Some(value.into()))]);
        assert!(validate_config_pairs("get_all", &unstripped, key, prefix, value).is_err());
        let incorrect_value = BTreeMap::from([("value".into(), Some("wrong".into()))]);
        assert!(validate_config_pairs("get_all", &incorrect_value, key, prefix, value).is_err());

        let registered = BTreeMap::from([(MODIFIABLE_CONFIG.into(), Some("true".into()))]);
        assert!(validate_config_pairs("is_modifiable", &registered, key, prefix, value).is_ok());
        let custom = BTreeMap::from([(key.into(), Some("true".into()))]);
        assert!(validate_config_pairs("is_modifiable", &custom, key, prefix, value).is_err());
        let unmodifiable = BTreeMap::from([(MODIFIABLE_CONFIG.into(), Some("false".into()))]);
        assert!(validate_config_pairs("is_modifiable", &unmodifiable, key, prefix, value).is_err());
    }

    #[test]
    fn response_validator_rejects_wrong_rows_offsets_bytes_and_duplicate_ids() {
        let schema = spark_connect::DataType::Struct {
            fields: vec![spark_connect::StructField {
                name: "id".into(),
                data_type: spark_connect::DataType::Long,
                nullable: false,
                metadata: Default::default(),
            }],
        }
        .to_proto();
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "id",
            std::sync::Arc::new(arrow::array::Int64Array::from(vec![1_i64]))
                as arrow::array::ArrayRef,
        )])
        .unwrap();
        let mut bytes = vec![];
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let payload = Payload {
            key: "range".into(),
            schema: schema.clone(),
            bytes,
            rows: 1,
        };
        let mut session = Session {
            id: Uuid::new_v4().to_string(),
            user: Default::default(),
            observed: None,
        };
        let operation = Uuid::new_v4().to_string();
        let mut responses = (0..3)
            .map(|_| proto::ExecutePlanResponse {
                session_id: session.id.clone(),
                server_side_session_id: session.id.clone(),
                operation_id: operation.clone(),
                response_id: Uuid::new_v4().to_string(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        responses[0].schema = Some(schema);
        responses[1].response_type = Some(proto::execute_plan_response::ResponseType::ArrowBatch(
            proto::execute_plan_response::ArrowBatch {
                row_count: 1,
                data: payload.bytes.clone().into(),
                start_offset: Some(0),
                ..Default::default()
            },
        ));
        responses[2].response_type = Some(
            proto::execute_plan_response::ResponseType::ResultComplete(Default::default()),
        );
        assert!(validate_stream(&mut session, &operation, &responses, &payload, 1).is_ok());
        let mut duplicate = responses.clone();
        duplicate[2].response_id = duplicate[1].response_id.clone();
        assert!(validate_stream(&mut session, &operation, &duplicate, &payload, 1).is_err());
        for field in ["rows", "offset", "bytes"] {
            let mut wrong = responses.clone();
            if let Some(proto::execute_plan_response::ResponseType::ArrowBatch(batch)) =
                &mut wrong[1].response_type
            {
                match field {
                    "rows" => batch.row_count = 2,
                    "offset" => batch.start_offset = Some(1),
                    _ => batch.data = vec![0].into(),
                }
            }
            assert!(validate_stream(&mut session, &operation, &wrong, &payload, 1).is_err());
        }
    }
}
