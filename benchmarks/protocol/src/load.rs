//! Closed-loop load: prepare first, measure primary RPCs, validate and clean up last.
use crate::corpus::{self, PlanCase};
use futures::{future::join_all, StreamExt};
use p::spark_connect_service_client::SparkConnectServiceClient;
use prost::Message;
use serde_json::{json, Value};
use spark_connect_proto as p;
use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Barrier;
use tonic::{
    transport::{Channel, Endpoint},
    Code, Request, Status,
};
use uuid::Uuid;

type Client = SparkConnectServiceClient<Channel>;
const MAX_MESSAGE: usize = 64 * 1024 * 1024;
const CLIENT_TYPE: &str = "fork-pinned-rust/protocol-bench";
const CONFIG_KEY: &str = "protocol.bench.value";

fn get_status_compatibility(mode: &str, route: &str) -> Option<Value> {
    (mode == "fixture" && route == "GetStatus").then(|| {
        json!({"scope":"synthetic GetStatus compatibility",
            "codecs":"Rust fixture: prost; JVM fixture: shaded protobuf DynamicMessage",
            "descriptor_provenance":"JVM shutdown.fixture_stats.get_status_descriptor",
            "production_spark_frontend_comparison":false})
    })
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub endpoint: String,
    pub mode: String,
    pub profile: String,
    pub payload_dir: PathBuf,
    pub output: PathBuf,
    pub concurrency: Vec<usize>,
    pub session_models: Vec<String>,
    pub channel_models: Vec<String>,
    pub requests: usize,
    pub warmups: usize,
    pub repetitions: usize,
    pub timeout: Duration,
    pub client_threads: usize,
    pub case_filter: Option<String>,
    pub coverage_only: bool,
    pub max_prepared_bytes: usize,
}

fn settings_metadata(settings: &Settings) -> Value {
    // Retain the legacy key as an alias for old consumers. It identifies the
    // actual fork pin; official_base_revision identifies its upstream base.
    json!({
        "endpoint":settings.endpoint,"profile":settings.profile,"coverage_only":settings.coverage_only,"requests":settings.requests,"warmups":settings.warmups,"repetitions":settings.repetitions,"concurrency":settings.concurrency,"session_models":settings.session_models,"channel_models":settings.channel_models,"client_threads":settings.client_threads,"timeout_seconds":settings.timeout.as_secs_f64(),"case_filter":settings.case_filter,"max_prepared_bytes":settings.max_prepared_bytes,"closed_loop":true,"retries":0,"no_execution":true,"plan_building_outside_timing":true,"validation_outside_timing":true,"cleanup_outside_timing":true,
        "client_origin":"fork-pinned Rust client","client_revision":crate::api_audit::CLIENT_REVISION,
        "official_base_revision":crate::api_audit::OFFICIAL_BASE_REVISION,
        "official_rust_revision":crate::api_audit::CLIENT_REVISION,
        "official_rust_revision_alias_for":"client_revision",
        "protocol_version":"4.3.0","fixture_get_status_required":settings.mode=="fixture",
        "primary_rpc_allowlist":if settings.mode=="planning"{vec!["AnalyzePlan","Config"]}else{vec!["ExecutePlan","AnalyzePlan","Config","AddArtifacts","ArtifactStatus","Interrupt","ReattachExecute","ReleaseExecute","ReleaseSession","FetchErrorDetails","CloneSession","GetStatus"]}
    })
}
#[derive(Clone)]
struct Payload {
    key: String,
    schema: p::DataType,
    data: prost::bytes::Bytes,
    rows: i64,
}
#[derive(Clone)]
enum Kind {
    Analyze(Arc<PlanCase>),
    Config(u8),
    Stream(Arc<Payload>, usize),
    Replay(Arc<Payload>),
    ReleaseOperation,
    Interrupt,
    ReleaseSession,
    CloneSession,
    Upload(prost::bytes::Bytes, bool),
    ArtifactStatus,
    FetchError,
    GetStatus,
}
#[derive(Clone)]
struct Case {
    name: String,
    route: &'static str,
    kind: Kind,
    metadata: Value,
    memory_per_call: usize,
}
#[derive(Clone)]
struct Session {
    id: String,
    server_id: String,
}
struct Worker {
    client: Client,
    session: Session,
}
#[derive(Default)]
struct Resources(Mutex<BTreeMap<String, Client>>);
impl Resources {
    fn register(&self, id: &str, client: &Client) {
        self.0.lock().unwrap().insert(id.into(), client.clone());
    }
    fn forget(&self, id: &str) {
        self.0.lock().unwrap().remove(id);
    }
    async fn release(
        &self,
        client: &mut Client,
        id: &str,
        timeout: Duration,
    ) -> Result<(), Status> {
        let response = client
            .release_session(request(release_request(id), timeout))
            .await?
            .into_inner();
        if response.session_id != id {
            return Err(Status::internal(
                "session cleanup returned a different session id",
            ));
        }
        self.forget(id);
        Ok(())
    }
    async fn drain(&self, timeout: Duration) -> Vec<String> {
        let pending = std::mem::take(&mut *self.0.lock().unwrap());
        futures::stream::iter(pending)
            .map(|(id, mut client)| async move {
                client
                    .release_session(request(release_request(&id), timeout))
                    .await
                    .err()
                    .map(|e| format!("final session cleanup {id}: {e}"))
            })
            .buffer_unordered(32)
            .filter_map(|e| async move { e })
            .collect()
            .await
    }
}
#[derive(Clone)]
struct Identity {
    session: Session,
}

enum Input {
    Analyze(Box<p::AnalyzePlanRequest>),
    Config(p::ConfigRequest),
    Execute(Box<p::ExecutePlanRequest>),
    Replay(p::ReattachExecuteRequest),
    ReleaseOperation(p::ReleaseExecuteRequest),
    Interrupt(p::InterruptRequest),
    ReleaseSession(p::ReleaseSessionRequest),
    Clone(p::CloneSessionRequest),
    Upload(Vec<p::AddArtifactsRequest>),
    ArtifactStatus(p::ArtifactStatusesRequest),
    Fetch(p::FetchErrorDetailsRequest),
    Status(p::GetStatusRequest),
}
enum Raw {
    Analyze(p::AnalyzePlanResponse),
    Config(p::ConfigResponse),
    Stream(Vec<p::ExecutePlanResponse>),
    ReleaseOperation(p::ReleaseExecuteResponse),
    Interrupt(p::InterruptResponse),
    ReleaseSession(p::ReleaseSessionResponse),
    Clone(p::CloneSessionResponse),
    Upload(p::AddArtifactsResponse),
    ArtifactStatus(p::ArtifactStatusesResponse),
    Fetch(p::FetchErrorDetailsResponse),
    Status(p::GetStatusResponse),
}
struct Prepared {
    input: Option<Input>,
    identity: Identity,
    expected: Expected,
    cleanup_op: Option<String>,
    cleanup_session: Option<String>,
    held: Option<tonic::Streaming<p::ExecutePlanResponse>>,
    request_bytes: usize,
}
enum Expected {
    Schema(p::DataType),
    Config(u8),
    Stream(Arc<Payload>, usize, String, bool),
    Released(String),
    Interrupted(String),
    SessionReleased,
    Cloned(String),
    Uploaded(String),
    ArtifactAbsent,
    Error,
    Status,
}
struct Sample {
    prepared: Prepared,
    result: Result<Raw, Status>,
    elapsed_ns: u64,
    first_ns: Option<u64>,
    first_arrow_ns: Option<u64>,
    response_bytes: u64,
}
fn user() -> Option<p::UserContext> {
    Some(p::UserContext {
        user_id: "protocol-bench".into(),
        ..Default::default()
    })
}
fn plan(relation: p::Relation) -> Option<p::Plan> {
    Some(p::Plan {
        op_type: Some(p::plan::OpType::Root(relation)),
    })
}
fn range() -> p::Relation {
    spark_connect::plan::range(0, 1_000_000_000_000, 1).to_proto()
}
fn nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
fn cpu_ns() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // getrusage writes its initialized output only on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    let usage = unsafe { usage.assume_init() };
    ((usage.ru_utime.tv_sec as u64 + usage.ru_stime.tv_sec as u64) * 1_000_000
        + usage.ru_utime.tv_usec as u64
        + usage.ru_stime.tv_usec as u64)
        * 1000
}
fn request<T>(message: T, timeout: Duration) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(timeout);
    request
}
fn with_payload<T>(message: T, timeout: Duration, key: &str) -> Result<Request<T>, Status> {
    let mut request = request(message, timeout);
    request.metadata_mut().insert(
        "x-bench-payload",
        key.parse()
            .map_err(|_| Status::invalid_argument("invalid payload key"))?,
    );
    Ok(request)
}
fn release_request(id: &str) -> p::ReleaseSessionRequest {
    p::ReleaseSessionRequest {
        session_id: id.into(),
        user_context: user(),
        ..Default::default()
    }
}
fn release_operation(session: &Session, id: &str) -> p::ReleaseExecuteRequest {
    p::ReleaseExecuteRequest {
        session_id: session.id.clone(),
        client_observed_server_side_session_id: Some(session.server_id.clone()),
        user_context: user(),
        operation_id: id.into(),
        release: Some(p::release_execute_request::Release::ReleaseAll(
            Default::default(),
        )),
        ..Default::default()
    }
}
fn config_request(session: &Session, op: p::config_request::operation::OpType) -> p::ConfigRequest {
    p::ConfigRequest {
        session_id: session.id.clone(),
        user_context: user(),
        client_observed_server_side_session_id: (!session.server_id.is_empty())
            .then(|| session.server_id.clone()),
        client_type: Some(CLIENT_TYPE.into()),
        operation: Some(p::config_request::Operation { op_type: Some(op) }),
    }
}
fn config_op(index: u8) -> p::config_request::operation::OpType {
    use p::config_request::{operation::OpType, *};
    match index {
        0 => OpType::Get(Get {
            keys: vec!["spark.sql.session.timeZone".into()],
        }),
        1 => OpType::GetOption(GetOption {
            keys: vec!["protocol.bench.missing".into()],
        }),
        2 => OpType::GetWithDefault(GetWithDefault {
            pairs: vec![p::KeyValue {
                key: "protocol.bench.missing".into(),
                value: Some("fallback".into()),
            }],
        }),
        3 => OpType::GetAll(GetAll {
            prefix: Some("protocol.bench.".into()),
        }),
        4 => OpType::Set(Set {
            pairs: vec![p::KeyValue {
                key: CONFIG_KEY.into(),
                value: Some("π😀 fixed value".into()),
            }],
            ..Default::default()
        }),
        5 => OpType::Unset(Unset {
            keys: vec!["protocol.bench.missing".into()],
        }),
        _ => OpType::IsModifiable(IsModifiable {
            keys: vec!["spark.sql.session.timeZone".into()],
        }),
    }
}
async fn connect(endpoint: &str, timeout: Duration) -> Result<Client, String> {
    let endpoint = endpoint
        .strip_prefix("sc://")
        .map(|s| format!("http://{s}"))
        .unwrap_or_else(|| endpoint.into());
    let channel = Endpoint::from_shared(endpoint)
        .map_err(|e| e.to_string())?
        .connect_timeout(timeout)
        .tcp_nodelay(true)
        .connect()
        .await
        .map_err(|e| e.to_string())?;
    Ok(Client::new(channel)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE))
}
async fn open(
    client: &mut Client,
    timeout: Duration,
    resources: &Resources,
) -> Result<Session, String> {
    let mut session = Session {
        id: Uuid::new_v4().to_string(),
        server_id: String::new(),
    };
    resources.register(&session.id, client);
    let response = client
        .config(request(config_request(&session, config_op(0)), timeout))
        .await
        .map_err(|e| e.to_string())?
        .into_inner();
    if response.session_id != session.id
        || Uuid::parse_str(&response.server_side_session_id).is_err()
    {
        return Err("invalid Config session identity".into());
    }
    session.server_id = response.server_side_session_id;
    // Set a private key so GetAll has a stable expected value even under concurrency.
    client
        .config(request(config_request(&session, config_op(4)), timeout))
        .await
        .map_err(|e| e.to_string())?;
    Ok(session)
}
fn read_payloads(directory: &Path) -> Result<Vec<Arc<Payload>>, String> {
    let manifest: Value = serde_json::from_slice(
        &std::fs::read(directory.join("manifest.json")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    manifest["payloads"]
        .as_array()
        .ok_or("payload manifest missing entries")?
        .iter()
        .map(|item| {
            let key = item["key"]
                .as_str()
                .ok_or("payload key missing")?
                .to_owned();
            let schema = p::DataType::decode(
                std::fs::read(
                    directory.join(
                        item["schema_file"]
                            .as_str()
                            .ok_or("schema filename missing")?,
                    ),
                )
                .map_err(|e| e.to_string())?
                .as_slice(),
            )
            .map_err(|e| e.to_string())?;
            let data = std::fs::read(
                directory.join(item["arrow_file"].as_str().ok_or("IPC filename missing")?),
            )
            .map_err(|e| e.to_string())?
            .into();
            let rows = item["rows"].as_i64().ok_or("payload rows missing")?;
            Ok(Arc::new(Payload {
                key,
                schema,
                data,
                rows,
            }))
        })
        .collect()
}
fn cases(settings: &Settings, plans: Vec<PlanCase>, payloads: &[Arc<Payload>]) -> Vec<Case> {
    let fixture = settings.mode == "fixture";
    let mut cases = plans
        .into_iter()
        .map(|p| {
            let memory = p.relation.encoded_len().saturating_mul(2) + 8192;
            Case {
                name: format!("analyze/{}", p.name),
                route: "AnalyzePlan",
                metadata: p.metadata.clone(),
                kind: Kind::Analyze(Arc::new(p)),
                memory_per_call: memory,
            }
        })
        .collect::<Vec<_>>();
    for (index, name) in [
        "get",
        "get_option",
        "get_with_default",
        "get_all",
        "set",
        "unset",
        "is_modifiable",
    ]
    .iter()
    .enumerate()
    {
        cases.push(Case {
            name: format!("config/{name}"),
            route: "Config",
            kind: Kind::Config(index as u8),
            metadata: json!({"variant":name}),
            memory_per_call: 8192,
        });
    }
    if fixture {
        for payload in payloads {
            for batches in [1, 8] {
                if batches == 8 && payload.data.len() > 65536 {
                    continue;
                }
                cases.push(Case{name:format!("stream/{}/batches{batches}",payload.key),route:"ExecutePlan",kind:Kind::Stream(payload.clone(),batches),metadata:json!({"payload":payload.key,"ipc_bytes":payload.data.len(),"rows_per_batch":payload.rows,"batches":batches}),memory_per_call:payload.data.len().saturating_mul(batches)+16384});
            }
        }
        let base = payloads
            .iter()
            .find(|p| p.key == "range")
            .expect("range fallback")
            .clone();
        for (name, route, kind) in [
            ("reattach", "ReattachExecute", Kind::Replay(base)),
            ("release_execute", "ReleaseExecute", Kind::ReleaseOperation),
            ("interrupt", "Interrupt", Kind::Interrupt),
            ("release_session", "ReleaseSession", Kind::ReleaseSession),
            ("clone_session", "CloneSession", Kind::CloneSession),
            ("artifact_status", "ArtifactStatus", Kind::ArtifactStatus),
            ("fetch_error", "FetchErrorDetails", Kind::FetchError),
            ("get_status", "GetStatus", Kind::GetStatus),
        ] {
            cases.push(Case{name:format!("protocol/{name}"),route,kind,metadata:json!({"variant":name,"setup_cleanup_outside_timing":true,"per_request_session":name=="release_session"}),memory_per_call:16384});
        }
        for size in if settings.profile == "smoke" {
            vec![0, 4096]
        } else {
            vec![0, 4096, 65536, 1048576]
        } {
            let bytes: prost::bytes::Bytes = (0..size)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>()
                .into();
            for chunked in [false, true] {
                cases.push(Case {
                    name: format!(
                        "artifact/{}-{size}",
                        if chunked { "chunked" } else { "batch" }
                    ),
                    route: "AddArtifacts",
                    kind: Kind::Upload(bytes.clone(), chunked),
                    metadata: json!({"artifact_bytes":size,"chunked":chunked}),
                    memory_per_call: size + 16384,
                });
            }
        }
    }
    cases.retain(|c| {
        settings
            .case_filter
            .as_ref()
            .is_none_or(|f| c.name.contains(f))
    });
    cases
}
fn execute_request(
    session: &Session,
    payload: &Payload,
    batches: usize,
    hold: bool,
) -> p::ExecutePlanRequest {
    p::ExecutePlanRequest {
        session_id: session.id.clone(),
        user_context: user(),
        client_observed_server_side_session_id: Some(session.server_id.clone()),
        operation_id: Some(Uuid::new_v4().to_string()),
        plan: plan(range()),
        client_type: Some(CLIENT_TYPE.into()),
        tags: vec![
            format!("bench.payload={}", payload.key),
            format!("bench.batches={batches}"),
            format!("bench.hold={hold}"),
        ],
        ..Default::default()
    }
}
async fn drain(
    mut stream: tonic::Streaming<p::ExecutePlanResponse>,
) -> Result<Vec<p::ExecutePlanResponse>, Status> {
    let mut responses = Vec::new();
    let mut bytes = 0usize;
    while let Some(response) = stream.message().await? {
        bytes = bytes.saturating_add(response.encoded_len());
        if bytes > MAX_MESSAGE {
            return Err(Status::resource_exhausted(
                "benchmark response retention exceeds 64 MiB",
            ));
        }
        responses.push(response);
    }
    Ok(responses)
}
async fn prepare(
    case: &Case,
    worker: &mut Worker,
    payloads: &[Arc<Payload>],
    fixture: bool,
    timeout: Duration,
    resources: &Resources,
) -> Result<Prepared, String> {
    let mut session = worker.session.clone();
    let mut cleanup_op = None;
    let mut cleanup_session = None;
    let mut held = None;
    let base = payloads
        .iter()
        .find(|p| p.key == "range")
        .ok_or("range fallback missing")?;
    let (input, expected) = match &case.kind {
        Kind::Analyze(case) => {
            let schema = if fixture {
                base.schema.clone()
            } else {
                case.expected_schema.clone()
            };
            (
                Input::Analyze(Box::new(p::AnalyzePlanRequest {
                    session_id: session.id.clone(),
                    user_context: user(),
                    client_observed_server_side_session_id: Some(session.server_id.clone()),
                    client_type: Some(CLIENT_TYPE.into()),
                    analyze: Some(p::analyze_plan_request::Analyze::Schema(
                        p::analyze_plan_request::Schema {
                            plan: plan(case.relation.clone()),
                        },
                    )),
                })),
                Expected::Schema(schema),
            )
        }
        Kind::Config(index) => (
            Input::Config(config_request(&session, config_op(*index))),
            Expected::Config(*index),
        ),
        Kind::Stream(payload, batches) => {
            let request = execute_request(&session, payload, *batches, false);
            let op = request.operation_id.clone().unwrap();
            cleanup_op = Some(op.clone());
            (
                Input::Execute(Box::new(request)),
                Expected::Stream(payload.clone(), *batches, op, false),
            )
        }
        Kind::Replay(payload) => {
            let req = execute_request(&session, payload, 2, false);
            let op = req.operation_id.clone().unwrap();
            cleanup_op = Some(op.clone());
            let original = worker
                .client
                .execute_plan(request(req, timeout))
                .await
                .map_err(|e| e.to_string())?
                .into_inner();
            let responses = tokio::time::timeout(timeout, drain(original))
                .await
                .map_err(|_| "replay preparation timed out")?
                .map_err(|e| e.to_string())?;
            let header = responses
                .first()
                .filter(|r| r.schema.is_some())
                .ok_or("replay preparation lacks schema")?;
            (
                Input::Replay(p::ReattachExecuteRequest {
                    session_id: session.id.clone(),
                    user_context: user(),
                    client_observed_server_side_session_id: Some(session.server_id.clone()),
                    operation_id: op.clone(),
                    last_response_id: Some(header.response_id.clone()),
                    client_type: Some(CLIENT_TYPE.into()),
                }),
                Expected::Stream(payload.clone(), 2, op, true),
            )
        }
        Kind::ReleaseOperation | Kind::Interrupt => {
            let hold = matches!(case.kind, Kind::Interrupt);
            let req = execute_request(&session, base, 1, hold);
            let op = req.operation_id.clone().unwrap();
            cleanup_op = Some(op.clone());
            // A held stream lives across the preparation phase. Its lifetime
            // ends explicitly through Interrupt/release, not a consumed setup
            // gRPC deadline; local setup and primary calls stay time-bounded.
            let outbound = if hold {
                Request::new(req)
            } else {
                request(req, timeout)
            };
            let mut stream = tokio::time::timeout(timeout, worker.client.execute_plan(outbound))
                .await
                .map_err(|_| "lifecycle setup headers timed out")?
                .map_err(|e| e.to_string())?
                .into_inner();
            if hold {
                let first = tokio::time::timeout(timeout, stream.message())
                    .await
                    .map_err(|_| "held stream preparation timed out")?
                    .map_err(|e| e.to_string())?
                    .ok_or("held stream lacks header")?;
                if first.schema.is_none() {
                    return Err("held stream lacks schema".into());
                }
                held = Some(stream);
                (
                    Input::Interrupt(p::InterruptRequest {
                        session_id: session.id.clone(),
                        user_context: user(),
                        client_observed_server_side_session_id: Some(session.server_id.clone()),
                        interrupt_type: p::interrupt_request::InterruptType::OperationId as i32,
                        interrupt: Some(p::interrupt_request::Interrupt::OperationId(op.clone())),
                        client_type: Some(CLIENT_TYPE.into()),
                    }),
                    Expected::Interrupted(op),
                )
            } else {
                tokio::time::timeout(timeout, drain(stream))
                    .await
                    .map_err(|_| "release preparation timed out")?
                    .map_err(|e| e.to_string())?;
                (
                    Input::ReleaseOperation(release_operation(&session, &op)),
                    Expected::Released(op),
                )
            }
        }
        Kind::ReleaseSession => {
            session = open(&mut worker.client, timeout, resources).await?;
            cleanup_session = Some(session.id.clone());
            (
                Input::ReleaseSession(release_request(&session.id)),
                Expected::SessionReleased,
            )
        }
        Kind::CloneSession => {
            let id = Uuid::new_v4().to_string();
            cleanup_session = Some(id.clone());
            resources.register(&id, &worker.client);
            (
                Input::Clone(p::CloneSessionRequest {
                    session_id: session.id.clone(),
                    user_context: user(),
                    client_observed_server_side_session_id: Some(session.server_id.clone()),
                    client_type: Some(CLIENT_TYPE.into()),
                    new_session_id: Some(id.clone()),
                }),
                Expected::Cloned(id),
            )
        }
        Kind::Upload(bytes, chunked) => {
            use p::add_artifacts_request::{
                ArtifactChunk, Batch, BeginChunkedArtifact, Payload, SingleChunkArtifact,
            };
            let name = format!("cache/bench-{}.bin", Uuid::new_v4());
            let chunk = |data: prost::bytes::Bytes| ArtifactChunk {
                crc: i64::from(crc32fast::hash(&data)),
                data,
            };
            let payloads = if *chunked {
                let slices = if bytes.is_empty() {
                    vec![bytes.clone()]
                } else {
                    (0..bytes.len())
                        .step_by(16384)
                        .map(|n| bytes.slice(n..(n + 16384).min(bytes.len())))
                        .collect::<Vec<_>>()
                };
                let mut items = vec![Payload::BeginChunk(BeginChunkedArtifact {
                    name: name.clone(),
                    total_bytes: bytes.len() as i64,
                    num_chunks: slices.len() as i64,
                    initial_chunk: Some(chunk(slices[0].clone())),
                })];
                items.extend(slices.into_iter().skip(1).map(|b| Payload::Chunk(chunk(b))));
                items
            } else {
                vec![Payload::Batch(Batch {
                    artifacts: vec![SingleChunkArtifact {
                        name: name.clone(),
                        data: Some(chunk(bytes.clone())),
                    }],
                })]
            };
            let requests = payloads
                .into_iter()
                .map(|payload| p::AddArtifactsRequest {
                    session_id: session.id.clone(),
                    user_context: user(),
                    client_observed_server_side_session_id: Some(session.server_id.clone()),
                    client_type: Some(CLIENT_TYPE.into()),
                    payload: Some(payload),
                })
                .collect();
            (Input::Upload(requests), Expected::Uploaded(name))
        }
        Kind::ArtifactStatus => (
            Input::ArtifactStatus(p::ArtifactStatusesRequest {
                session_id: session.id.clone(),
                user_context: user(),
                client_observed_server_side_session_id: Some(session.server_id.clone()),
                names: vec!["cache/absent.bin".into()],
                client_type: Some(CLIENT_TYPE.into()),
            }),
            Expected::ArtifactAbsent,
        ),
        Kind::FetchError => (
            Input::Fetch(p::FetchErrorDetailsRequest {
                session_id: session.id.clone(),
                user_context: user(),
                client_observed_server_side_session_id: Some(session.server_id.clone()),
                error_id: "fixture-error".into(),
                client_type: Some(CLIENT_TYPE.into()),
            }),
            Expected::Error,
        ),
        Kind::GetStatus => (
            Input::Status(p::GetStatusRequest {
                session_id: session.id.clone(),
                user_context: user(),
                client_observed_server_side_session_id: Some(session.server_id.clone()),
                operation_status: Some(Default::default()),
                client_type: Some(CLIENT_TYPE.into()),
                ..Default::default()
            }),
            Expected::Status,
        ),
    };
    let request_bytes = match &input {
        Input::Analyze(r) => r.encoded_len(),
        Input::Config(r) => r.encoded_len(),
        Input::Execute(r) => r.encoded_len(),
        Input::Replay(r) => r.encoded_len(),
        Input::ReleaseOperation(r) => r.encoded_len(),
        Input::Interrupt(r) => r.encoded_len(),
        Input::ReleaseSession(r) => r.encoded_len(),
        Input::Clone(r) => r.encoded_len(),
        Input::Upload(r) => r.iter().map(Message::encoded_len).sum(),
        Input::ArtifactStatus(r) => r.encoded_len(),
        Input::Fetch(r) => r.encoded_len(),
        Input::Status(r) => r.encoded_len(),
    };
    Ok(Prepared {
        input: Some(input),
        identity: Identity { session },
        expected,
        cleanup_op,
        cleanup_session,
        held,
        request_bytes,
    })
}
async fn invoke(client: &mut Client, mut prepared: Prepared, timeout: Duration) -> Sample {
    let input = prepared
        .input
        .take()
        .expect("one call per prepared request");
    let start = Instant::now();
    let mut first_ns = None;
    let mut first_arrow_ns = None;
    let mut observed_stream_bytes = 0u64;
    let result = tokio::time::timeout(timeout, async {
        macro_rules! unary {
            ($method:ident,$r:expr,$variant:ident) => {
                client
                    .$method(request($r, timeout))
                    .await
                    .map(|r| Raw::$variant(r.into_inner()))
            };
        }
        match input {
            Input::Analyze(r) => client
                .analyze_plan(with_payload(*r, timeout, "range")?)
                .await
                .map(|r| Raw::Analyze(r.into_inner())),
            Input::Config(r) => unary!(config, r, Config),
            Input::ReleaseOperation(r) => unary!(release_execute, r, ReleaseOperation),
            Input::Interrupt(r) => unary!(interrupt, r, Interrupt),
            Input::ReleaseSession(r) => unary!(release_session, r, ReleaseSession),
            Input::Clone(r) => unary!(clone_session, r, Clone),
            Input::Upload(r) => client
                .add_artifacts(request(tokio_stream::iter(r), timeout))
                .await
                .map(|r| Raw::Upload(r.into_inner())),
            Input::ArtifactStatus(r) => unary!(artifact_status, r, ArtifactStatus),
            Input::Fetch(r) => unary!(fetch_error_details, r, Fetch),
            Input::Status(r) => unary!(get_status, r, Status),
            Input::Execute(r) => {
                let mut stream = client
                    .execute_plan(request(*r, timeout))
                    .await?
                    .into_inner();
                let mut rows = Vec::new();
                let mut bytes = 0;
                while let Some(response) = stream.message().await? {
                    if first_ns.is_none() {
                        first_ns = Some(nanos(start));
                    }
                    if first_arrow_ns.is_none()
                        && matches!(
                            response.response_type,
                            Some(p::execute_plan_response::ResponseType::ArrowBatch(_))
                        )
                    {
                        first_arrow_ns = Some(nanos(start));
                    }
                    bytes += response.encoded_len();
                    observed_stream_bytes = bytes as u64;
                    if bytes > MAX_MESSAGE {
                        return Err(Status::resource_exhausted("response collection limit"));
                    }
                    rows.push(response);
                }
                Ok(Raw::Stream(rows))
            }
            Input::Replay(r) => {
                let mut stream = client
                    .reattach_execute(request(r, timeout))
                    .await?
                    .into_inner();
                let mut rows = Vec::new();
                let mut bytes = 0;
                while let Some(response) = stream.message().await? {
                    if first_ns.is_none() {
                        first_ns = Some(nanos(start));
                    }
                    if first_arrow_ns.is_none()
                        && matches!(
                            response.response_type,
                            Some(p::execute_plan_response::ResponseType::ArrowBatch(_))
                        )
                    {
                        first_arrow_ns = Some(nanos(start));
                    }
                    bytes += response.encoded_len();
                    observed_stream_bytes = bytes as u64;
                    if bytes > MAX_MESSAGE {
                        return Err(Status::resource_exhausted("response collection limit"));
                    }
                    rows.push(response);
                }
                Ok(Raw::Stream(rows))
            }
        }
    })
    .await
    .unwrap_or_else(|_| Err(Status::deadline_exceeded("benchmark whole-call timeout")));
    let elapsed_ns = nanos(start);
    let response_bytes = result
        .as_ref()
        .map_or(observed_stream_bytes as usize, |r| match r {
            Raw::Analyze(r) => r.encoded_len(),
            Raw::Config(r) => r.encoded_len(),
            Raw::Stream(r) => r.iter().map(Message::encoded_len).sum(),
            Raw::ReleaseOperation(r) => r.encoded_len(),
            Raw::Interrupt(r) => r.encoded_len(),
            Raw::ReleaseSession(r) => r.encoded_len(),
            Raw::Clone(r) => r.encoded_len(),
            Raw::Upload(r) => r.encoded_len(),
            Raw::ArtifactStatus(r) => r.encoded_len(),
            Raw::Fetch(r) => r.encoded_len(),
            Raw::Status(r) => r.encoded_len(),
        }) as u64;
    Sample {
        prepared,
        result,
        elapsed_ns,
        first_ns,
        first_arrow_ns,
        response_bytes,
    }
}
fn normalized_schema(schema: &p::DataType) -> Result<spark_connect::DataType, String> {
    fn normalize(kind: &mut spark_connect::DataType) {
        use spark_connect::DataType as T;
        match kind {
            T::String { collation } if collation.is_empty() => *collation = "UTF8_BINARY".into(),
            T::Array { element_type, .. } => normalize(element_type),
            T::Map {
                key_type,
                value_type,
                ..
            } => {
                normalize(key_type);
                normalize(value_type);
            }
            T::Struct { fields } => {
                for f in fields {
                    f.metadata.clear();
                    normalize(&mut f.data_type);
                }
            }
            _ => {}
        }
    }
    let mut parsed = spark_connect::DataType::from_proto(schema).map_err(|e| e.to_string())?;
    normalize(&mut parsed);
    Ok(parsed)
}
fn identity(expected: &Identity, session: &str, server: &str) -> Result<(), String> {
    if session != expected.session.id || server != expected.session.server_id {
        Err("response session/server identity differs".into())
    } else {
        Ok(())
    }
}
fn check(sample: &Sample) -> Result<(), String> {
    let raw = sample.result.as_ref().map_err(|e| e.to_string())?;
    let id = &sample.prepared.identity;
    macro_rules! ident {
        ($r:expr) => {
            identity(id, &$r.session_id, &$r.server_side_session_id)?
        };
    }
    match (raw, &sample.prepared.expected) {
        (Raw::Analyze(r), Expected::Schema(expected)) => {
            ident!(r);
            let Some(p::analyze_plan_response::Result::Schema(schema)) = &r.result else {
                return Err("analysis response lacks schema variant".into());
            };
            let actual = schema.schema.as_ref().ok_or("analysis schema missing")?;
            if normalized_schema(actual)? != normalized_schema(expected)? {
                return Err(format!(
                    "analysis schema mismatch: actual={actual:?}, expected={expected:?}"
                ));
            }
        }
        (Raw::Config(r), Expected::Config(index)) => {
            ident!(r);
            let pairs = r
                .pairs
                .iter()
                .map(|p| (p.key.as_str(), p.value.as_deref()))
                .collect::<BTreeMap<_, _>>();
            let expected = match index {
                0 => BTreeMap::from([("spark.sql.session.timeZone", Some("UTC"))]),
                1 => BTreeMap::from([("protocol.bench.missing", None)]),
                2 => BTreeMap::from([("protocol.bench.missing", Some("fallback"))]),
                3 => BTreeMap::from([("value", Some("π😀 fixed value"))]),
                4 | 5 => BTreeMap::new(),
                _ => BTreeMap::from([("spark.sql.session.timeZone", Some("true"))]),
            };
            if pairs != expected || pairs.len() != r.pairs.len() {
                return Err(format!("Config {index} values differ: {pairs:?}"));
            }
        }
        (Raw::Stream(responses), Expected::Stream(payload, batches, operation, replay)) => {
            if !*replay && responses.first().is_none_or(|r| r.schema.is_none()) {
                return Err("stream must start with its schema".into());
            }
            if responses.len() != batches + if *replay { 1 } else { 2 } {
                return Err("stream response count differs".into());
            }
            let mut seen = HashSet::new();
            let mut batch_count = 0;
            let mut complete = 0;
            let mut schema_count = 0;
            for response in responses {
                identity(id, &response.session_id, &response.server_side_session_id)?;
                if response.operation_id != *operation
                    || Uuid::parse_str(&response.response_id).is_err()
                    || !seen.insert(&response.response_id)
                {
                    return Err("stream operation/response identity differs".into());
                }
                if let Some(schema) = &response.schema {
                    schema_count += 1;
                    if normalized_schema(schema)? != normalized_schema(&payload.schema)? {
                        return Err("stream declared schema differs".into());
                    }
                }
                match &response.response_type {
                    Some(p::execute_plan_response::ResponseType::ArrowBatch(batch)) => {
                        if batch.row_count != payload.rows
                            || batch.data != payload.data
                            || batch.start_offset != Some(payload.rows * batch_count as i64)
                        {
                            return Err(
                                "stream Arrow bytes/rows/offset differ from prebuilt payload"
                                    .into(),
                            );
                        }
                        batch_count += 1;
                    }
                    Some(p::execute_plan_response::ResponseType::ResultComplete(_)) => {
                        complete += 1
                    }
                    None if response.schema.is_some() => {}
                    _ => return Err("unexpected stream response variant".into()),
                }
            }
            if batch_count != *batches
                || complete != 1
                || schema_count != usize::from(!*replay)
                || !matches!(
                    responses.last().and_then(|r| r.response_type.as_ref()),
                    Some(p::execute_plan_response::ResponseType::ResultComplete(_))
                )
            {
                return Err("stream schema/batch/completion ordering differs".into());
            }
        }
        (Raw::ReleaseOperation(r), Expected::Released(op)) => {
            ident!(r);
            if r.operation_id.as_deref() != Some(op) {
                return Err("released operation missing".into());
            }
        }
        (Raw::Interrupt(r), Expected::Interrupted(op)) => {
            ident!(r);
            if r.interrupted_ids != [op.clone()] {
                return Err("interrupted operation missing".into());
            }
        }
        (Raw::ReleaseSession(r), Expected::SessionReleased) => {
            ident!(r);
        }
        (Raw::Clone(r), Expected::Cloned(new)) => {
            ident!(r);
            if r.new_session_id != *new || Uuid::parse_str(&r.new_server_side_session_id).is_err() {
                return Err("cloned session identity invalid".into());
            }
        }
        (Raw::Upload(r), Expected::Uploaded(name)) => {
            ident!(r);
            if r.artifacts.len() != 1
                || r.artifacts[0].name != *name
                || !r.artifacts[0].is_crc_successful
            {
                return Err("artifact upload CRC/name differs".into());
            }
        }
        (Raw::ArtifactStatus(r), Expected::ArtifactAbsent) => {
            ident!(r);
            if r.statuses.len() != 1 || r.statuses.get("cache/absent.bin").is_none_or(|s| s.exists)
            {
                return Err("absent artifact status differs".into());
            }
        }
        (Raw::Fetch(r), Expected::Error) => {
            ident!(r);
            if r.root_error_idx != Some(0)
                || r.errors.len() != 1
                || r.errors[0].message != "synthetic protocol fixture error"
            {
                return Err("structured error root/message differs".into());
            }
        }
        (Raw::Status(r), Expected::Status) => {
            ident!(r);
        }
        _ => return Err("response variant differs from request".into()),
    }
    Ok(())
}
async fn cleanup(
    client: &mut Client,
    sample: &mut Sample,
    timeout: Duration,
    resources: &Resources,
) -> Vec<String> {
    let mut errors = Vec::new();
    if let Some(mut held) = sample.prepared.held.take() {
        if sample.result.is_ok() {
            let terminal = tokio::time::timeout(timeout, held.message()).await;
            if !matches!(terminal,Ok(Err(ref s)) if s.code()==Code::Cancelled) {
                errors.push("held stream did not terminate CANCELLED after Interrupt".into());
            }
        }
        drop(held);
    }
    if let Some(op) = &sample.prepared.cleanup_op {
        if let Err(e) = client
            .release_execute(request(
                release_operation(&sample.prepared.identity.session, op),
                timeout,
            ))
            .await
        {
            errors.push(format!("ReleaseExecute cleanup: {e}"));
        }
    }
    if let Some(id) = &sample.prepared.cleanup_session {
        if matches!(sample.prepared.expected, Expected::SessionReleased) && sample.result.is_ok() {
            resources.forget(id);
        } else if let Err(e) = resources.release(client, id, timeout).await {
            errors.push(format!("ReleaseSession cleanup: {e}"));
        }
    }
    errors
}
async fn close_workers(
    workers: &mut [Worker],
    timeout: Duration,
    resources: &Resources,
) -> Vec<String> {
    let mut ids = HashSet::new();
    let mut errors = Vec::new();
    for worker in workers {
        if ids.insert(worker.session.id.clone()) {
            if let Err(e) = resources
                .release(&mut worker.client, &worker.session.id, timeout)
                .await
            {
                errors.push(e.to_string());
            }
        }
    }
    errors
}
async fn workers(
    settings: &Settings,
    count: usize,
    sessions: &str,
    channels: &str,
    resources: &Resources,
) -> Result<Vec<Worker>, String> {
    let mut base = connect(&settings.endpoint, settings.timeout).await?;
    let shared = open(&mut base, settings.timeout, resources).await?;
    let mut out = Vec::new();
    for index in 0..count {
        let mut client = if channels == "isolated" {
            connect(&settings.endpoint, settings.timeout).await?
        } else {
            base.clone()
        };
        let session = if sessions == "isolated" && index > 0 {
            open(&mut client, settings.timeout, resources).await?
        } else {
            shared.clone()
        };
        out.push(Worker { client, session });
    }
    Ok(out)
}
struct Round {
    samples: Vec<Sample>,
    preparation_ns: u64,
    elapsed_ns: u64,
    cpu_ns: u64,
}
async fn round(
    workers: &mut [Worker],
    case: &Case,
    payloads: &[Arc<Payload>],
    count: usize,
    settings: &Settings,
    resources: &Resources,
) -> Result<Round, String> {
    let preparation_started = Instant::now();
    let concurrency = workers.len();
    let mut prepared = Vec::new();
    for (index, worker) in workers.iter_mut().enumerate() {
        let requests = count / concurrency + usize::from(index < count % concurrency);
        let mut calls = Vec::with_capacity(requests);
        for _ in 0..requests {
            if preparation_started.elapsed() > settings.timeout {
                return Err("request preparation exceeded phase timeout".into());
            }
            calls.push(
                prepare(
                    case,
                    worker,
                    payloads,
                    settings.mode == "fixture",
                    settings.timeout,
                    resources,
                )
                .await?,
            );
        }
        prepared.push(calls);
    }
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let futures = workers
        .iter()
        .zip(prepared)
        .map(|(worker, calls)| {
            let mut client = worker.client.clone();
            let barrier = barrier.clone();
            let timeout = settings.timeout;
            tokio::spawn(async move {
                barrier.wait().await;
                let mut results = Vec::with_capacity(calls.len());
                for call in calls {
                    results.push(invoke(&mut client, call, timeout).await);
                }
                results
            })
        })
        .collect::<Vec<_>>();
    let cpu = cpu_ns();
    let preparation_ns = nanos(preparation_started);
    let started = Instant::now();
    barrier.wait().await;
    let results = join_all(futures).await;
    let elapsed_ns = nanos(started);
    let cpu_ns = cpu_ns().saturating_sub(cpu);
    let mut samples = Vec::with_capacity(count);
    for result in results {
        samples.extend(result.map_err(|e| format!("load worker failed: {e}"))?);
    }
    Ok(Round {
        samples,
        preparation_ns,
        elapsed_ns,
        cpu_ns,
    })
}
async fn validate_round(
    round: &mut Round,
    workers: &mut [Worker],
    timeout: Duration,
    resources: &Resources,
    support_required: bool,
) -> (
    Vec<String>,
    Vec<String>,
    usize,
    usize,
    usize,
    BTreeMap<String, usize>,
) {
    let mut validation = Vec::new();
    let mut cleanup_errors = Vec::new();
    let mut ok = 0;
    let mut failures = 0;
    let mut unsupported = 0;
    let mut codes = BTreeMap::new();
    for sample in &mut round.samples {
        let status = sample.result.as_ref().err().map_or(Code::Ok, Status::code);
        *codes.entry(format!("{status:?}")).or_default() += 1;
        if status == Code::Unimplemented && !support_required {
            unsupported += 1;
        } else if let Err(e) = check(sample) {
            failures += 1;
            if validation.len() < 16 {
                validation.push(e);
            }
        } else {
            ok += 1;
        }
        cleanup_errors.extend(cleanup(&mut workers[0].client, sample, timeout, resources).await);
    }
    (validation, cleanup_errors, ok, failures, unsupported, codes)
}
pub async fn run(settings: &Settings) -> Result<Value, String> {
    if settings.output.exists() {
        return Err("benchmark output must be a new file".into());
    }
    let resources = Resources::default();
    let result = run_inner(settings, &resources).await;
    // Every opened or possibly-created session is registered before its first
    // RPC. Releasing these also retires operations if preparation failed midway.
    let cleanup_errors = resources.drain(settings.timeout).await;
    let mut report = match result {
        Ok(report) => report,
        Err(error) => {
            let mut partial = std::fs::read(&settings.output)
                .ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                .unwrap_or_else(|| json!({"schema_version":1,"mode":settings.mode,"cells":[]}));
            partial["passed"] = json!(false);
            partial["error"] = json!(error);
            partial["incomplete"] = json!(true);
            partial
        }
    };
    if !cleanup_errors.is_empty() {
        report["passed"] = json!(false);
    }
    report["final_cleanup_errors"] = json!(cleanup_errors);
    save(&settings.output, &report)?;
    Ok(report)
}
async fn run_inner(settings: &Settings, resources: &Resources) -> Result<Value, String> {
    if !matches!(settings.mode.as_str(), "fixture" | "planning") {
        return Err("mode must be fixture or planning".into());
    }
    if settings.requests == 0
        || settings.concurrency.is_empty()
        || settings
            .concurrency
            .iter()
            .any(|c| *c == 0 || *c > 512 || *c > settings.requests)
    {
        return Err("requests must be >= every concurrency (1..512)".into());
    }
    if settings.repetitions == 0
        || settings.session_models.is_empty()
        || settings.channel_models.is_empty()
        || settings
            .session_models
            .iter()
            .chain(&settings.channel_models)
            .any(|v| !matches!(v.as_str(), "shared" | "isolated"))
    {
        return Err("invalid repetitions or topology".into());
    }
    let plans = corpus::build(&settings.profile)?;
    let payloads = read_payloads(&settings.payload_dir)?;
    let cases = cases(settings, plans, &payloads);
    if cases.is_empty() && !settings.coverage_only {
        return Err("case filter selected no cases".into());
    }
    let coverage = crate::probes::run(
        &settings.endpoint,
        &settings.mode,
        &settings.payload_dir,
        settings.timeout,
    )
    .await?;
    let mut report = json!({"schema_version":1,"mode":settings.mode,"passed":coverage["passed"]==true,"settings":settings_metadata(settings),"coverage":coverage,"capabilities":[],"skipped_cells":[],"cells":[]});
    save(&settings.output, &report)?;
    if !settings.coverage_only {
        for case in cases {
            report["in_progress"] = json!({"case":case.name,"phase":"capability"});
            save(&settings.output, &report)?;
            let mut probe_workers = workers(settings, 1, "shared", "shared", resources).await?;
            let mut probe =
                round(&mut probe_workers, &case, &payloads, 1, settings, resources).await?;
            let (validation, mut clean, ok, failures, unsupported, _) = validate_round(
                &mut probe,
                &mut probe_workers,
                settings.timeout,
                resources,
                settings.mode == "fixture" && case.route == "GetStatus",
            )
            .await;
            clean.extend(close_workers(&mut probe_workers, settings.timeout, resources).await);
            let compatibility = get_status_compatibility(&settings.mode, case.route);
            let capability = json!({"case":case.name,"rpc":case.route,"supported":ok==1,"unsupported":unsupported,"validation_errors":validation,"cleanup_errors":clean,"protocol_implementation":compatibility});
            report["capabilities"]
                .as_array_mut()
                .unwrap()
                .push(capability);
            if failures > 0 || !clean.is_empty() {
                report["passed"] = json!(false);
                continue;
            }
            if unsupported > 0 {
                continue;
            }
            for &concurrency in &settings.concurrency {
                for session_model in &settings.session_models {
                    for channel_model in &settings.channel_models {
                        for repetition in 0..settings.repetitions {
                            let stateful_limit =
                                matches!(
                                    case.kind,
                                    Kind::Stream(..)
                                        | Kind::Replay(_)
                                        | Kind::ReleaseOperation
                                        | Kind::Interrupt
                                ) && settings.requests.max(settings.warmups.max(concurrency))
                                    > 4096;
                            let artifact_limit = matches!(case.kind, Kind::Upload(..))
                                && settings
                                    .requests
                                    .saturating_add(settings.warmups.max(concurrency))
                                    > 4096;
                            if stateful_limit || artifact_limit {
                                report["skipped_cells"].as_array_mut().unwrap().push(json!({"case":case.name,"concurrency":concurrency,"session_model":session_model,"channel_model":channel_model,"repetition":repetition,"reason":"fixture 4096 retained operations/artifacts per session limit"}));
                                continue;
                            }
                            let retained = case.memory_per_call.saturating_mul(
                                settings.requests.max(settings.warmups.max(concurrency)),
                            );
                            if retained > settings.max_prepared_bytes {
                                report["skipped_cells"].as_array_mut().unwrap().push(json!({"case":case.name,"concurrency":concurrency,"session_model":session_model,"channel_model":channel_model,"repetition":repetition,"reason":"prepared request/response memory budget","estimated_bytes":retained}));
                                continue;
                            }
                            let mut workers = workers(
                                settings,
                                concurrency,
                                session_model,
                                channel_model,
                                resources,
                            )
                            .await?;
                            let warmup_count = settings.warmups.max(concurrency);
                            let mut warmup = round(
                                &mut workers,
                                &case,
                                &payloads,
                                warmup_count,
                                settings,
                                resources,
                            )
                            .await?;
                            let (warm_errors, warm_cleanup, _, warm_fail, warm_unsupported, _) =
                                validate_round(
                                    &mut warmup,
                                    &mut workers,
                                    settings.timeout,
                                    resources,
                                    settings.mode == "fixture" && case.route == "GetStatus",
                                )
                                .await;
                            drop(warmup);
                            let mut measured = round(
                                &mut workers,
                                &case,
                                &payloads,
                                settings.requests,
                                settings,
                                resources,
                            )
                            .await?;
                            let first = measured
                                .samples
                                .iter()
                                .filter_map(|s| s.first_ns)
                                .collect::<Vec<_>>();
                            let first_arrow = measured
                                .samples
                                .iter()
                                .filter_map(|s| s.first_arrow_ns)
                                .collect::<Vec<_>>();
                            let latencies = measured
                                .samples
                                .iter()
                                .map(|s| s.elapsed_ns)
                                .collect::<Vec<_>>();
                            let request_bytes = measured
                                .samples
                                .iter()
                                .map(|s| s.prepared.request_bytes as u64)
                                .sum::<u64>();
                            let response_bytes = measured
                                .samples
                                .iter()
                                .map(|s| s.response_bytes)
                                .sum::<u64>();
                            let after = Instant::now();
                            let (
                                errors,
                                mut cleanup_errors,
                                successes,
                                failures,
                                unsupported,
                                status_counts,
                            ) = validate_round(
                                &mut measured,
                                &mut workers,
                                settings.timeout,
                                resources,
                                settings.mode == "fixture" && case.route == "GetStatus",
                            )
                            .await;
                            cleanup_errors.extend(
                                close_workers(&mut workers, settings.timeout, resources).await,
                            );
                            let validation_cleanup_ns = nanos(after);
                            if failures > 0
                                || unsupported > 0
                                || warm_fail > 0
                                || warm_unsupported > 0
                                || !cleanup_errors.is_empty()
                                || !warm_cleanup.is_empty()
                            {
                                report["passed"] = json!(false);
                            }
                            report["cells"].as_array_mut().unwrap().push(json!({"case":case.name,"workload":case.route,"metadata":case.metadata,"protocol_implementation":compatibility,"session_model":session_model,"channel_model":channel_model,"concurrency":concurrency,"repetition":repetition,"requests":settings.requests,"warmup_requests":warmup_count,"successes":successes,"failures":failures,"unsupported":unsupported,"elapsed_ns":measured.elapsed_ns,"preparation_ns":measured.preparation_ns,"full_round_elapsed_ns":measured.preparation_ns.saturating_add(measured.elapsed_ns).saturating_add(validation_cleanup_ns),"latencies_ns":latencies,"first_message_latencies_ns":first,"first_arrow_latencies_ns":first_arrow,"request_bytes":request_bytes,"response_bytes":response_bytes,"byte_accounting":"protobuf encoded bytes, excluding gRPC/HTTP2 framing","status_counts":status_counts,"validation_errors":errors,"cleanup_errors":cleanup_errors,"warmup_validation_errors":warm_errors,"warmup_cleanup_errors":warm_cleanup,"client_cpu_ns":measured.cpu_ns,"validation_cleanup_ns":validation_cleanup_ns,"latency_window":"primary_rpc_drain_including_client_encode","throughput_window":"primary_rpc_round_excluding_preparation_validation_cleanup"}));
                            save(&settings.output, &report)?;
                        }
                    }
                }
            }
            println!("PROTOCOL_BENCH_CASE_COMPLETE {}", case.name);
            save(&settings.output, &report)?;
        }
    }
    report["in_progress"] = Value::Null;
    save(&settings.output, &report)?;
    Ok(report)
}
fn save(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let mut temp = path.as_os_str().to_os_string();
    temp.push(".tmp");
    let temp = PathBuf::from(temp);
    std::fs::write(
        &temp,
        serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    std::fs::rename(temp, path).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_variants_cover_every_operation() {
        let variants = (0..7)
            .map(|i| format!("{:?}", config_op(i)))
            .collect::<HashSet<_>>();
        assert_eq!(variants.len(), 7);
    }

    #[tokio::test]
    async fn required_fixture_status_unimplemented_is_a_validation_failure() {
        let session = Session {
            id: Uuid::new_v4().to_string(),
            server_id: Uuid::new_v4().to_string(),
        };
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let mut workers = [Worker {
            client: Client::new(channel),
            session: session.clone(),
        }];
        let sample = || Sample {
            prepared: Prepared {
                input: None,
                identity: Identity {
                    session: session.clone(),
                },
                expected: Expected::Status,
                cleanup_op: None,
                cleanup_session: None,
                held: None,
                request_bytes: 0,
            },
            result: Err(Status::unimplemented("fixture route disappeared")),
            elapsed_ns: 1,
            first_ns: None,
            first_arrow_ns: None,
            response_bytes: 0,
        };
        for (required, expected_failures, expected_unsupported) in [(true, 1, 0), (false, 0, 1)] {
            let mut round = Round {
                samples: vec![sample()],
                preparation_ns: 0,
                elapsed_ns: 1,
                cpu_ns: 0,
            };
            let (errors, cleanup, successes, failures, unsupported, codes) = validate_round(
                &mut round,
                &mut workers,
                Duration::from_secs(1),
                &Resources::default(),
                required,
            )
            .await;
            assert_eq!(
                (successes, failures, unsupported),
                (0, expected_failures, expected_unsupported)
            );
            assert_eq!(errors.len(), expected_failures);
            assert!(cleanup.is_empty());
            assert_eq!(codes.get("Unimplemented"), Some(&1));
        }
        let annotation = get_status_compatibility("fixture", "GetStatus").unwrap();
        assert_eq!(annotation["production_spark_frontend_comparison"], false);
        assert!(get_status_compatibility("planning", "GetStatus").is_none());
    }
    fn test_settings(mode: &str) -> Settings {
        Settings {
            endpoint: String::new(),
            mode: mode.into(),
            profile: "smoke".into(),
            payload_dir: PathBuf::new(),
            output: PathBuf::new(),
            concurrency: vec![1],
            session_models: vec!["shared".into()],
            channel_models: vec!["shared".into()],
            requests: 1,
            warmups: 1,
            repetitions: 1,
            timeout: Duration::from_secs(1),
            client_threads: 1,
            case_filter: None,
            coverage_only: false,
            max_prepared_bytes: 1 << 20,
        }
    }

    #[test]
    fn report_settings_identify_fork_pin_upstream_base_and_legacy_alias() {
        for mode in ["fixture", "planning"] {
            let metadata = settings_metadata(&test_settings(mode));
            assert_eq!(
                metadata["client_revision"],
                crate::api_audit::CLIENT_REVISION
            );
            assert_eq!(
                metadata["official_rust_revision"],
                crate::api_audit::CLIENT_REVISION
            );
            assert_eq!(
                metadata["official_base_revision"],
                crate::api_audit::OFFICIAL_BASE_REVISION
            );
            assert_eq!(
                metadata["official_rust_revision_alias_for"],
                "client_revision"
            );
            assert_eq!(metadata["fixture_get_status_required"], mode == "fixture");
        }
    }

    #[test]
    fn planning_case_allowlist_cannot_dispatch_execution() {
        let s = test_settings("planning");
        let result = cases(&s, corpus::build("smoke").unwrap(), &[]);
        assert!(result
            .iter()
            .all(|c| matches!(c.kind, Kind::Analyze(_) | Kind::Config(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resource_tracker_releases_sessions_after_partial_lifecycle_preparation() {
        use crate::fixture::Fixture;
        use crate::grpc::spark_connect_service_server::SparkConnectServiceServer;
        use tokio_stream::wrappers::TcpListenerStream;

        struct TemporaryDirectory(PathBuf);
        impl Drop for TemporaryDirectory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        struct RunningServer {
            task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
            fixture: Arc<Fixture>,
        }
        impl Drop for RunningServer {
            fn drop(&mut self) {
                self.fixture.shutdown();
                self.task.abort();
            }
        }

        let directory = TemporaryDirectory(
            std::env::temp_dir().join(format!("protocol-partial-preparation-{}", Uuid::new_v4())),
        );
        corpus::export(&corpus::build("smoke").unwrap(), &directory.0).unwrap();
        let payloads = read_payloads(&directory.0).unwrap();
        let fixture = Arc::new(Fixture::load(&directory.0.join("manifest.json")).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("protocol integration tests require a loopback listener");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut server = RunningServer {
            fixture: fixture.clone(),
            task: tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(SparkConnectServiceServer::from_arc(fixture.clone()))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                        let _ = stopped.await;
                    }),
            ),
        };
        let timeout = Duration::from_secs(2);
        let resources = Resources::default();
        let mut client = connect(&endpoint, timeout).await.unwrap();
        let session = open(&mut client, timeout, &resources).await.unwrap();
        let mut worker = Worker { client, session };
        let interrupt = Case {
            name: "test/held-operation".into(),
            route: "Interrupt",
            kind: Kind::Interrupt,
            metadata: Value::Null,
            memory_per_call: 16384,
        };
        let held = prepare(
            &interrupt,
            &mut worker,
            &payloads,
            true,
            timeout,
            &resources,
        )
        .await
        .unwrap();
        assert!(held.held.is_some());
        assert_eq!(fixture.stats()["retained_operations"], 1);

        let base = payloads
            .iter()
            .find(|payload| payload.key == "range")
            .unwrap();
        let invalid_replay = Case {
            name: "test/missing-replay-payload".into(),
            route: "ReattachExecute",
            kind: Kind::Replay(Arc::new(Payload {
                key: "missing".into(),
                schema: base.schema.clone(),
                data: base.data.clone(),
                rows: base.rows,
            })),
            metadata: Value::Null,
            memory_per_call: 16384,
        };
        assert!(prepare(
            &invalid_replay,
            &mut worker,
            &payloads,
            true,
            timeout,
            &resources,
        )
        .await
        .is_err());

        // Dropping the client stream does not release the retained operation.
        // The tracker must clean up even though no measured Sample was created.
        drop(held);
        assert_eq!(fixture.stats()["sessions"], 1);
        assert_eq!(fixture.stats()["retained_operations"], 1);
        assert!(resources.drain(timeout).await.is_empty());
        let stats = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let stats = fixture.stats();
                if [
                    "sessions",
                    "retained_operations",
                    "live_operations",
                    "artifacts",
                ]
                .iter()
                .all(|field| stats[field] == 0)
                {
                    break stats;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("partial preparation cleanup left retained fixture state");
        assert_eq!(stats["query_executions"], 0);
        assert!(resources.0.lock().unwrap().is_empty());
        let _ = stop.send(());
        tokio::time::timeout(Duration::from_secs(5), &mut server.task)
            .await
            .expect("fixture listener did not stop")
            .unwrap()
            .unwrap();
    }
}
