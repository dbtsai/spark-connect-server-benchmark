//! Synthetic Spark Connect protocol fixture. No plan is resolved or executed.
//!
//! Result IPC and Spark schemas are loaded before serving. This exercises the
//! generated tonic decoder, envelopes, lifecycle state and streaming transport;
//! it deliberately does not represent Catalyst or DataFusion execution.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Component, Path};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::{stream, Stream};
use prost::Message;
use serde_json::{json, Value};
use spark_connect_proto as proto;
use tokio::sync::watch;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::grpc::spark_connect_service_server::SparkConnectService;

const MAX_ACTIVE_SESSIONS: usize = 8192;
const MAX_SESSION_IDENTITIES: usize = 131072;
const MAX_OPERATIONS: usize = 4096;
const MAX_GLOBAL_OPERATIONS: usize = 16384;
const MAX_ARTIFACTS: usize = 4096;
const MAX_PAYLOAD: usize = 8 * 1024 * 1024;
const MAX_PRELOAD: usize = 64 * 1024 * 1024;
const MAX_UPLOAD: usize = 64 * 1024 * 1024;
const ROUTES: [&str; 12] = [
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

type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<proto::ExecutePlanResponse, Status>> + Send>>;
type SessionKey = (String, String);

#[derive(Clone)]
pub struct FixturePayload {
    pub schema: proto::DataType,
    pub ipc: proto::execute_plan_response::ArrowBatch,
}

struct Operation {
    _permit: OperationPermit,
    responses: Arc<Vec<proto::ExecutePlanResponse>>,
    released_through: Mutex<Option<usize>>,
    cancelled: watch::Sender<bool>,
    state: AtomicUsize,
    tags: Vec<String>,
    hold: bool,
}

struct OperationPermit(Arc<AtomicUsize>);
impl Drop for OperationPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Operation {
    fn cancel(&self) {
        self.state.store(6, Ordering::Release);
        self.cancelled.send_replace(true);
    }

    fn stream(self: &Arc<Self>, start: usize) -> ResponseStream {
        let operation = self.clone();
        let cancelled = self.cancelled.subscribe();
        Box::pin(stream::unfold(
            (operation, cancelled, start, false),
            |(operation, mut cancelled, index, ended)| async move {
                if ended {
                    return None;
                }
                if *cancelled.borrow() {
                    return Some((
                        Err(Status::cancelled("fixture operation interrupted")),
                        (operation, cancelled, index, true),
                    ));
                }
                // Holding after the schema gives lifecycle tests an operation
                // which cannot finish before a release/interrupt is observed.
                if operation.hold && index > 0 {
                    let _ = cancelled.changed().await;
                    return Some((
                        Err(Status::cancelled("fixture operation interrupted")),
                        (operation, cancelled, index, true),
                    ));
                }
                let response = operation.responses.get(index)?.clone();
                Some((Ok(response), (operation, cancelled, index + 1, false)))
            },
        ))
    }
}

#[derive(Default)]
struct SessionData {
    configs: BTreeMap<String, String>,
    artifacts: HashMap<String, usize>,
    operations: HashMap<String, Arc<Operation>>,
}

struct Session {
    id: String,
    released: AtomicBool,
    data: Mutex<SessionData>,
}

impl Session {
    fn new(id: String) -> Self {
        Self {
            id,
            released: AtomicBool::new(false),
            data: Mutex::new(SessionData {
                configs: BTreeMap::from([
                    ("spark.sql.session.timeZone".into(), "UTC".into()),
                    ("spark.sql.shuffle.partitions".into(), "2".into()),
                ]),
                ..Default::default()
            }),
        }
    }

    fn ensure_open(&self) -> Result<(), Status> {
        if self.released.load(Ordering::Acquire) {
            Err(Status::failed_precondition("fixture session is released"))
        } else {
            Ok(())
        }
    }
}

#[derive(Default)]
struct Registry {
    sessions: HashMap<SessionKey, Arc<Session>>,
    tombstones: HashSet<SessionKey>,
}

pub struct Fixture {
    payloads: HashMap<String, FixturePayload>,
    registry: Mutex<Registry>,
    calls: [AtomicU64; 12],
    operation_count: Arc<AtomicUsize>,
}

impl Fixture {
    pub fn new(payloads: HashMap<String, FixturePayload>) -> Result<Self, String> {
        if !payloads.contains_key("range") {
            return Err("fixture payloads require the range fallback".into());
        }
        let total = payloads.values().try_fold(0usize, |total, payload| {
            if payload.ipc.data.len() > MAX_PAYLOAD || payload.ipc.row_count < 0 {
                return Err("fixture payload exceeds 8 MiB or has invalid rows".to_owned());
            }
            let mut reader = arrow::ipc::reader::StreamReader::try_new(
                std::io::Cursor::new(payload.ipc.data.as_ref()),
                None,
            )
            .map_err(|error| format!("invalid fixture Arrow IPC: {error}"))?;
            let rows = reader.try_fold(0usize, |rows, batch| {
                let batch = batch.map_err(|error| error.to_string())?;
                rows.checked_add(batch.num_rows())
                    .ok_or_else(|| "fixture row count overflow".to_owned())
            })?;
            if rows as u128 != payload.ipc.row_count as u128 {
                return Err("fixture row count differs from Arrow IPC".into());
            }
            total
                .checked_add(payload.ipc.data.len())
                .ok_or_else(|| "payload size overflow".into())
        })?;
        if total > MAX_PRELOAD {
            return Err("fixture preload exceeds 64 MiB".into());
        }
        Ok(Self {
            payloads,
            registry: Mutex::new(Registry::default()),
            calls: std::array::from_fn(|_| AtomicU64::new(0)),
            operation_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Accepts a JSON manifest with a `payloads` array. Paths are relative to it.
    pub fn load(manifest_path: &Path) -> Result<Self, String> {
        let manifest: Value =
            serde_json::from_slice(&std::fs::read(manifest_path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        let entries = manifest
            .get("payloads")
            .or_else(|| manifest.as_array().map(|_| &manifest))
            .and_then(Value::as_array)
            .ok_or("fixture manifest needs a payloads array")?;
        let parent = manifest_path.parent().ok_or("manifest has no parent")?;
        let mut payloads = HashMap::new();
        for entry in entries {
            let key = entry
                .get("key")
                .and_then(Value::as_str)
                .ok_or("payload key missing")?;
            let schema_file = entry
                .get("schema_file")
                .and_then(Value::as_str)
                .ok_or("schema_file missing")?;
            let arrow_file = entry
                .get("arrow_file")
                .and_then(Value::as_str)
                .ok_or("arrow_file missing")?;
            let rows = entry
                .get("rows")
                .and_then(Value::as_i64)
                .ok_or("payload rows missing")?;
            // Fixture manifests are local benchmark artifacts, but accidental
            // path escapes should not read unrelated files into a network server.
            relative_name(schema_file).map_err(|e| e.to_string())?;
            relative_name(arrow_file).map_err(|e| e.to_string())?;
            let schema_bytes =
                std::fs::read(parent.join(schema_file)).map_err(|e| e.to_string())?;
            let schema =
                proto::DataType::decode(schema_bytes.as_slice()).map_err(|e| e.to_string())?;
            let data = std::fs::read(parent.join(arrow_file)).map_err(|e| e.to_string())?;
            let payload = FixturePayload {
                schema,
                ipc: proto::execute_plan_response::ArrowBatch {
                    row_count: rows,
                    data: data.into(),
                    ..Default::default()
                },
            };
            if payloads.insert(key.into(), payload).is_some() {
                return Err("duplicate fixture payload key".into());
            }
        }
        Self::new(payloads)
    }

    pub fn stats(&self) -> Value {
        let registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut operations = 0;
        let mut artifacts = 0;
        let mut artifact_bytes = 0;
        for session in registry.sessions.values() {
            let data = session
                .data
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            operations += data.operations.len();
            artifacts += data.artifacts.len();
            artifact_bytes += data.artifacts.values().sum::<usize>();
        }
        let routes = ROUTES
            .iter()
            .enumerate()
            .map(|(i, name)| {
                (
                    (*name).to_owned(),
                    json!(self.calls[i].load(Ordering::Relaxed)),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        json!({"profile":"synthetic-protocol-fixture", "query_executions":0,
            "sessions":registry.sessions.len(), "tombstones":registry.tombstones.len(),
            "retained_operations":operations, "artifacts":artifacts,
            "live_operations":self.operation_count.load(Ordering::Acquire),
            "verified_artifact_bytes":artifact_bytes, "calls":routes})
    }

    pub fn shutdown(&self) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for session in registry.sessions.values() {
            session.released.store(true, Ordering::Release);
            for operation in session
                .data
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .operations
                .values()
            {
                operation.cancel();
            }
        }
        registry.sessions.clear();
    }

    fn called(&self, index: usize) {
        self.calls[index].fetch_add(1, Ordering::Relaxed);
    }

    fn key(user: &Option<proto::UserContext>, id: &str) -> Result<SessionKey, Status> {
        Uuid::parse_str(id).map_err(|_| Status::invalid_argument("session_id must be a UUID"))?;
        let user = user
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("user_context is required"))?;
        Ok((user.user_id.clone(), id.to_owned()))
    }

    fn session(
        &self,
        user: &Option<proto::UserContext>,
        id: &str,
        observed: &Option<String>,
    ) -> Result<Arc<Session>, Status> {
        let key = Self::key(user, id)?;
        if observed.as_ref().is_some_and(|observed| observed != id) {
            return Err(Status::failed_precondition(
                "fixture server session id differs",
            ));
        }
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry.tombstones.contains(&key) {
            return Err(Status::failed_precondition("fixture session is released"));
        }
        if let Some(session) = registry.sessions.get(&key) {
            session.ensure_open()?;
            return Ok(session.clone());
        }
        if registry.sessions.len() >= MAX_ACTIVE_SESSIONS
            || registry.sessions.len() + registry.tombstones.len() >= MAX_SESSION_IDENTITIES
        {
            return Err(Status::resource_exhausted("fixture session limit"));
        }
        let session = Arc::new(Session::new(id.to_owned()));
        registry.sessions.insert(key, session.clone());
        Ok(session)
    }

    fn payload<'a, T>(
        &'a self,
        request: &Request<T>,
        tags: &[String],
    ) -> Result<&'a FixturePayload, Status> {
        let from_tag = tags
            .iter()
            .find_map(|tag| tag.strip_prefix("bench.payload="));
        let from_header = request
            .metadata()
            .get("x-bench-payload")
            .and_then(|value| value.to_str().ok());
        self.payloads
            .get(from_tag.or(from_header).unwrap_or("range"))
            .ok_or_else(|| Status::not_found("unknown fixture payload"))
    }
}

fn relative_name(name: &str) -> Result<(), Status> {
    if name.is_empty()
        || name.len() > 4096
        || Path::new(name)
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(Status::invalid_argument(
            "artifact/file name must be a relative path",
        ));
    }
    Ok(())
}

fn response_id(index: usize) -> String {
    format!("00000000-0000-4000-8000-{index:012x}")
}

fn envelope(id: &str, operation: &str, index: usize) -> proto::ExecutePlanResponse {
    proto::ExecutePlanResponse {
        session_id: id.into(),
        server_side_session_id: id.into(),
        operation_id: operation.into(),
        response_id: response_id(index),
        ..Default::default()
    }
}

#[tonic::async_trait]
impl SparkConnectService for Fixture {
    type ExecutePlanStream = ResponseStream;
    type ReattachExecuteStream = ResponseStream;

    async fn execute_plan(
        &self,
        request: Request<proto::ExecutePlanRequest>,
    ) -> Result<Response<ResponseStream>, Status> {
        self.called(0);
        let input = request.get_ref();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        if input
            .plan
            .as_ref()
            .and_then(|plan| plan.op_type.as_ref())
            .is_none()
        {
            return Err(Status::invalid_argument("plan is required"));
        }
        if input
            .tags
            .iter()
            .any(|tag| tag.is_empty() || tag.contains(','))
        {
            return Err(Status::invalid_argument("invalid operation tag"));
        }
        let payload = self.payload(&request, &input.tags)?;
        let batches = input
            .tags
            .iter()
            .find_map(|tag| tag.strip_prefix("bench.batches="))
            .unwrap_or("1")
            .parse::<usize>()
            .map_err(|_| Status::invalid_argument("invalid batch count"))?;
        if !(1..=64).contains(&batches) {
            return Err(Status::invalid_argument("batch count must be 1..64"));
        }
        let hold = input.tags.iter().any(|tag| tag == "bench.hold=true");
        let operation_id = input
            .operation_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        Uuid::parse_str(&operation_id)
            .map_err(|_| Status::invalid_argument("operation_id must be a UUID"))?;
        let mut count = self.operation_count.load(Ordering::Acquire);
        loop {
            if count >= MAX_GLOBAL_OPERATIONS {
                return Err(Status::resource_exhausted("fixture global operation limit"));
            }
            match self.operation_count.compare_exchange_weak(
                count,
                count + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => count = actual,
            }
        }
        let permit = OperationPermit(self.operation_count.clone());
        let mut responses = Vec::with_capacity(batches + 2);
        let mut header = envelope(&session.id, &operation_id, 0);
        header.schema = Some(payload.schema.clone());
        responses.push(header);
        for index in 0..batches {
            let mut response = envelope(&session.id, &operation_id, index + 1);
            let mut batch = payload.ipc.clone();
            batch.start_offset = Some(
                payload
                    .ipc
                    .row_count
                    .checked_mul(index as i64)
                    .ok_or_else(|| Status::resource_exhausted("row offset overflow"))?,
            );
            response.response_type = Some(proto::execute_plan_response::ResponseType::ArrowBatch(
                batch,
            ));
            responses.push(response);
        }
        let mut complete = envelope(&session.id, &operation_id, batches + 1);
        complete.response_type = Some(proto::execute_plan_response::ResponseType::ResultComplete(
            Default::default(),
        ));
        responses.push(complete);
        let (cancelled, _) = watch::channel(false);
        let operation = Arc::new(Operation {
            _permit: permit,
            responses: Arc::new(responses),
            released_through: Mutex::new(None),
            cancelled,
            state: AtomicUsize::new(if hold { 2 } else { 4 }),
            tags: input.tags.clone(),
            hold,
        });
        let mut data = session
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        session.ensure_open()?;
        if data.operations.contains_key(&operation_id) {
            return Err(Status::already_exists("fixture operation already exists"));
        }
        if data.operations.len() >= MAX_OPERATIONS {
            return Err(Status::resource_exhausted("fixture operation limit"));
        }
        data.operations.insert(operation_id, operation.clone());
        Ok(Response::new(operation.stream(0)))
    }

    async fn analyze_plan(
        &self,
        request: Request<proto::AnalyzePlanRequest>,
    ) -> Result<Response<proto::AnalyzePlanResponse>, Status> {
        self.called(1);
        let input = request.get_ref();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        let payload = self.payload(&request, &[])?;
        use proto::{analyze_plan_request::Analyze, analyze_plan_response as output};
        let result = match &input.analyze {
            Some(Analyze::Schema(schema)) => {
                if schema
                    .plan
                    .as_ref()
                    .and_then(|plan| plan.op_type.as_ref())
                    .is_none()
                {
                    return Err(Status::invalid_argument("analysis plan is required"));
                }
                output::Result::Schema(output::Schema {
                    schema: Some(payload.schema.clone()),
                })
            }
            Some(Analyze::IsLocal(_)) => {
                output::Result::IsLocal(output::IsLocal { is_local: true })
            }
            Some(Analyze::IsStreaming(_)) => output::Result::IsStreaming(output::IsStreaming {
                is_streaming: false,
            }),
            Some(Analyze::SparkVersion(_)) => output::Result::SparkVersion(output::SparkVersion {
                version: "protocol-fixture/4.3".into(),
            }),
            None => return Err(Status::invalid_argument("analysis is required")),
            _ => {
                return Err(Status::unimplemented(
                    "fixture does not resolve plans, DDL or SQL",
                ))
            }
        };
        Ok(Response::new(proto::AnalyzePlanResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.id.clone(),
            result: Some(result),
        }))
    }

    async fn config(
        &self,
        request: Request<proto::ConfigRequest>,
    ) -> Result<Response<proto::ConfigResponse>, Status> {
        self.called(2);
        let input = request.into_inner();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        let operation = input
            .operation
            .and_then(|operation| operation.op_type)
            .ok_or_else(|| Status::invalid_argument("config operation is required"))?;
        let mut data = session
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        session.ensure_open()?;
        use proto::config_request::operation::OpType;
        let pairs = match operation {
            OpType::Set(set) => {
                if set
                    .pairs
                    .iter()
                    .any(|pair| pair.key.is_empty() || pair.value.is_none())
                {
                    return Err(Status::invalid_argument("config key/value missing"));
                }
                if data.configs.len() + set.pairs.len() > MAX_ARTIFACTS {
                    return Err(Status::resource_exhausted("fixture config limit"));
                }
                for pair in set.pairs {
                    data.configs.insert(pair.key, pair.value.unwrap());
                }
                vec![]
            }
            OpType::Get(get) => get
                .keys
                .into_iter()
                .map(|key| {
                    let value = data
                        .configs
                        .get(&key)
                        .cloned()
                        .ok_or_else(|| Status::not_found("fixture config key missing"))?;
                    Ok(proto::KeyValue {
                        key,
                        value: Some(value),
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?,
            OpType::GetOption(get) => get
                .keys
                .into_iter()
                .map(|key| proto::KeyValue {
                    value: data.configs.get(&key).cloned(),
                    key,
                })
                .collect(),
            OpType::GetWithDefault(get) => get
                .pairs
                .into_iter()
                .map(|pair| proto::KeyValue {
                    value: data.configs.get(&pair.key).cloned().or(pair.value),
                    key: pair.key,
                })
                .collect(),
            OpType::GetAll(get) => {
                let prefix = get.prefix.as_deref().unwrap_or("");
                data.configs
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(prefix).map(|key| proto::KeyValue {
                            key: key.into(),
                            value: Some(value.clone()),
                        })
                    })
                    .collect()
            }
            OpType::Unset(unset) => {
                for key in unset.keys {
                    data.configs.remove(&key);
                }
                vec![]
            }
            OpType::IsModifiable(get) => get
                .keys
                .into_iter()
                .map(|key| proto::KeyValue {
                    key,
                    value: Some("true".into()),
                })
                .collect(),
        };
        Ok(Response::new(proto::ConfigResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.id.clone(),
            pairs,
            warnings: vec![],
        }))
    }

    async fn add_artifacts(
        &self,
        request: Request<tonic::Streaming<proto::AddArtifactsRequest>>,
    ) -> Result<Response<proto::AddArtifactsResponse>, Status> {
        self.called(3);
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("artifact stream is empty"))?;
        let session = self.session(
            &first.user_context,
            &first.session_id,
            &first.client_observed_server_side_session_id,
        )?;
        let user = first.user_context.clone();
        let mut upload = Upload::default();
        upload.accept(first.payload)?;
        while let Some(message) = stream.message().await? {
            if message.session_id != session.id || message.user_context != user {
                return Err(Status::invalid_argument(
                    "artifact stream changed session identity",
                ));
            }
            if message
                .client_observed_server_side_session_id
                .as_ref()
                .is_some_and(|id| id != &session.id)
            {
                return Err(Status::failed_precondition(
                    "fixture server session id differs",
                ));
            }
            session.ensure_open()?;
            upload.accept(message.payload)?;
        }
        if upload.current.is_some() {
            return Err(Status::invalid_argument("incomplete chunked artifact"));
        }
        let mut data = session
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        session.ensure_open()?;
        if data.artifacts.len() + upload.staged.len() > MAX_ARTIFACTS {
            return Err(Status::resource_exhausted("fixture artifact limit"));
        }
        data.artifacts.extend(upload.staged);
        Ok(Response::new(proto::AddArtifactsResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.id.clone(),
            artifacts: upload.summaries,
        }))
    }

    async fn artifact_status(
        &self,
        request: Request<proto::ArtifactStatusesRequest>,
    ) -> Result<Response<proto::ArtifactStatusesResponse>, Status> {
        self.called(4);
        let input = request.into_inner();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        let data = session
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        session.ensure_open()?;
        let statuses = input
            .names
            .into_iter()
            .map(|name| {
                let exists = data.artifacts.contains_key(&name);
                (
                    name,
                    proto::artifact_statuses_response::ArtifactStatus { exists },
                )
            })
            .collect();
        Ok(Response::new(proto::ArtifactStatusesResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.id.clone(),
            statuses,
        }))
    }

    async fn interrupt(
        &self,
        request: Request<proto::InterruptRequest>,
    ) -> Result<Response<proto::InterruptResponse>, Status> {
        self.called(5);
        let input = request.into_inner();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        let data = session
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        session.ensure_open()?;
        use proto::interrupt_request::Interrupt;
        let selected: Box<dyn Fn(&str, &Operation) -> bool> =
            match (input.interrupt_type, input.interrupt) {
                (1, _) => Box::new(|_, _| true),
                (2, Some(Interrupt::OperationTag(tag))) if !tag.is_empty() => {
                    Box::new(move |_, operation| operation.tags.contains(&tag))
                }
                (3, Some(Interrupt::OperationId(id))) => {
                    Box::new(move |operation_id, _| operation_id == id)
                }
                _ => return Err(Status::invalid_argument("invalid interrupt selector")),
            };
        let mut interrupted_ids = vec![];
        for (id, operation) in &data.operations {
            if operation.state.load(Ordering::Acquire) == 2 && selected(id, operation) {
                operation.cancel();
                interrupted_ids.push(id.clone());
            }
        }
        interrupted_ids.sort();
        Ok(Response::new(proto::InterruptResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.id.clone(),
            interrupted_ids,
        }))
    }

    async fn reattach_execute(
        &self,
        request: Request<proto::ReattachExecuteRequest>,
    ) -> Result<Response<ResponseStream>, Status> {
        self.called(6);
        let input = request.into_inner();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        let data = session
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        session.ensure_open()?;
        let operation = data
            .operations
            .get(&input.operation_id)
            .ok_or_else(|| Status::not_found("fixture operation missing"))?
            .clone();
        let start = match input.last_response_id {
            Some(id) => operation
                .responses
                .iter()
                .position(|response| response.response_id == id)
                .map(|index| index + 1)
                .ok_or_else(|| Status::not_found("fixture response missing"))?,
            None => 0,
        };
        if operation
            .released_through
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some_and(|released| start <= released)
        {
            return Err(Status::not_found("fixture response was released"));
        }
        Ok(Response::new(operation.stream(start)))
    }

    async fn release_execute(
        &self,
        request: Request<proto::ReleaseExecuteRequest>,
    ) -> Result<Response<proto::ReleaseExecuteResponse>, Status> {
        self.called(7);
        let input = request.into_inner();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        let mut data = session
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        session.ensure_open()?;
        let found = data.operations.get(&input.operation_id).cloned();
        use proto::release_execute_request::Release;
        match input.release {
            Some(Release::ReleaseAll(_)) => {
                if let Some(operation) = data.operations.remove(&input.operation_id) {
                    operation.cancel();
                }
            }
            Some(Release::ReleaseUntil(until)) => {
                if let Some(operation) = &found {
                    if let Some(index) = operation
                        .responses
                        .iter()
                        .position(|response| response.response_id == until.response_id)
                    {
                        let mut released = operation
                            .released_through
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        *released = Some(released.map_or(index, |previous| previous.max(index)));
                    }
                }
            }
            None => return Err(Status::invalid_argument("release selector is required")),
        }
        Ok(Response::new(proto::ReleaseExecuteResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.id.clone(),
            operation_id: found.map(|_| input.operation_id),
        }))
    }

    async fn release_session(
        &self,
        request: Request<proto::ReleaseSessionRequest>,
    ) -> Result<Response<proto::ReleaseSessionResponse>, Status> {
        self.called(8);
        let input = request.into_inner();
        let key = Self::key(&input.user_context, &input.session_id)?;
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !registry.sessions.contains_key(&key)
            && !registry.tombstones.contains(&key)
            && registry.sessions.len() + registry.tombstones.len() >= MAX_SESSION_IDENTITIES
        {
            return Err(Status::resource_exhausted("fixture session limit"));
        }
        if let Some(session) = registry.sessions.remove(&key) {
            session.released.store(true, Ordering::Release);
            for operation in session
                .data
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .operations
                .values()
            {
                operation.cancel();
            }
        }
        if input.allow_reconnect {
            registry.tombstones.remove(&key);
        } else {
            registry.tombstones.insert(key);
        }
        Ok(Response::new(proto::ReleaseSessionResponse {
            session_id: input.session_id.clone(),
            server_side_session_id: input.session_id,
        }))
    }

    async fn fetch_error_details(
        &self,
        request: Request<proto::FetchErrorDetailsRequest>,
    ) -> Result<Response<proto::FetchErrorDetailsResponse>, Status> {
        self.called(9);
        let input = request.into_inner();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        if input.error_id.is_empty() {
            return Err(Status::invalid_argument("error_id is required"));
        }
        let errors = if input.error_id == "fixture-error" {
            vec![proto::fetch_error_details_response::Error {
                error_type_hierarchy: vec![
                    "ProtocolFixtureError".into(),
                    "java.lang.RuntimeException".into(),
                ],
                message: "synthetic protocol fixture error".into(),
                stack_trace: vec![proto::fetch_error_details_response::StackTraceElement {
                    declaring_class: "ProtocolFixture".into(),
                    method_name: "decode".into(),
                    file_name: Some("fixture".into()),
                    line_number: 1,
                }],
                spark_throwable: Some(proto::fetch_error_details_response::SparkThrowable {
                    error_class: Some("FIXTURE_ERROR".into()),
                    sql_state: Some("XX000".into()),
                    message_parameters: HashMap::from([("kind".into(), "synthetic".into())]),
                    ..Default::default()
                }),
                ..Default::default()
            }]
        } else {
            vec![]
        };
        Ok(Response::new(proto::FetchErrorDetailsResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.id.clone(),
            root_error_idx: (!errors.is_empty()).then_some(0),
            errors,
        }))
    }

    async fn clone_session(
        &self,
        request: Request<proto::CloneSessionRequest>,
    ) -> Result<Response<proto::CloneSessionResponse>, Status> {
        self.called(10);
        let input = request.into_inner();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        let new_id = input
            .new_session_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let new_key = Self::key(&input.user_context, &new_id)?;
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        session.ensure_open()?;
        if registry.sessions.contains_key(&new_key) || registry.tombstones.contains(&new_key) {
            return Err(Status::already_exists(
                "fixture cloned session already exists",
            ));
        }
        if registry.sessions.len() >= MAX_ACTIVE_SESSIONS
            || registry.sessions.len() + registry.tombstones.len() >= MAX_SESSION_IDENTITIES
        {
            return Err(Status::resource_exhausted("fixture session limit"));
        }
        let data = session
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cloned = Session::new(new_id.clone());
        {
            let mut cloned_data = cloned
                .data
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cloned_data.configs = data.configs.clone();
            cloned_data.artifacts = data.artifacts.clone();
        }
        registry.sessions.insert(new_key, Arc::new(cloned));
        Ok(Response::new(proto::CloneSessionResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.id.clone(),
            new_session_id: new_id.clone(),
            new_server_side_session_id: new_id,
        }))
    }

    async fn get_status(
        &self,
        request: Request<proto::GetStatusRequest>,
    ) -> Result<Response<proto::GetStatusResponse>, Status> {
        self.called(11);
        let input = request.into_inner();
        let session = self.session(
            &input.user_context,
            &input.session_id,
            &input.client_observed_server_side_session_id,
        )?;
        let data = session
            .data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        session.ensure_open()?;
        let mut operation_statuses = vec![];
        if let Some(selector) = input.operation_status {
            let mut ids = selector.operation_ids;
            if ids.is_empty() {
                ids = data.operations.keys().cloned().collect();
                ids.sort();
            }
            let mut seen = HashSet::new();
            for id in ids {
                if !seen.insert(id.clone()) {
                    continue;
                }
                let state = data
                    .operations
                    .get(&id)
                    .map(|operation| operation.state.load(Ordering::Acquire) as i32)
                    .unwrap_or(1);
                operation_statuses.push(proto::get_status_response::OperationStatus {
                    operation_id: id,
                    state,
                    ..Default::default()
                });
            }
        }
        Ok(Response::new(proto::GetStatusResponse {
            session_id: session.id.clone(),
            server_side_session_id: session.id.clone(),
            operation_statuses,
            ..Default::default()
        }))
    }
}

#[derive(Default)]
struct Upload {
    bytes: usize,
    summaries: Vec<proto::add_artifacts_response::ArtifactSummary>,
    staged: HashMap<String, usize>,
    current: Option<Chunked>,
}

struct Chunked {
    name: String,
    expected_bytes: usize,
    expected_chunks: usize,
    bytes: usize,
    chunks: usize,
    crc_ok: bool,
}

impl Upload {
    fn chunk(
        &mut self,
        chunk: &proto::add_artifacts_request::ArtifactChunk,
    ) -> Result<bool, Status> {
        self.bytes = self
            .bytes
            .checked_add(chunk.data.len())
            .ok_or_else(|| Status::resource_exhausted("artifact size overflow"))?;
        if chunk.data.len() > MAX_PAYLOAD || self.bytes > MAX_UPLOAD {
            return Err(Status::resource_exhausted("fixture artifact upload limit"));
        }
        Ok(i64::from(crc32fast::hash(&chunk.data)) == chunk.crc)
    }

    fn complete(&mut self, name: String, bytes: usize, crc_ok: bool) -> Result<(), Status> {
        if self.summaries.len() >= MAX_ARTIFACTS {
            return Err(Status::resource_exhausted("fixture artifact count limit"));
        }
        if crc_ok {
            self.staged.insert(name.clone(), bytes);
        }
        self.summaries
            .push(proto::add_artifacts_response::ArtifactSummary {
                name,
                is_crc_successful: crc_ok,
            });
        Ok(())
    }

    fn accept(
        &mut self,
        payload: Option<proto::add_artifacts_request::Payload>,
    ) -> Result<(), Status> {
        use proto::add_artifacts_request::Payload;
        match payload.ok_or_else(|| Status::invalid_argument("artifact payload missing"))? {
            Payload::Batch(batch) => {
                if self.current.is_some() {
                    return Err(Status::invalid_argument("chunked artifact incomplete"));
                }
                for artifact in batch.artifacts {
                    relative_name(&artifact.name)?;
                    let chunk = artifact
                        .data
                        .ok_or_else(|| Status::invalid_argument("artifact data missing"))?;
                    let crc_ok = self.chunk(&chunk)?;
                    self.complete(artifact.name, chunk.data.len(), crc_ok)?;
                }
            }
            Payload::BeginChunk(begin) => {
                if self.current.is_some() {
                    return Err(Status::invalid_argument("chunked artifact incomplete"));
                }
                relative_name(&begin.name)?;
                let expected_bytes = usize::try_from(begin.total_bytes)
                    .map_err(|_| Status::invalid_argument("negative artifact size"))?;
                let expected_chunks = usize::try_from(begin.num_chunks)
                    .map_err(|_| Status::invalid_argument("negative chunk count"))?;
                if expected_chunks == 0 || expected_chunks > 65536 || expected_bytes > MAX_UPLOAD {
                    return Err(Status::resource_exhausted(
                        "fixture chunked artifact dimensions",
                    ));
                }
                let chunk = begin
                    .initial_chunk
                    .ok_or_else(|| Status::invalid_argument("initial artifact chunk missing"))?;
                let crc_ok = self.chunk(&chunk)?;
                self.current = Some(Chunked {
                    name: begin.name,
                    expected_bytes,
                    expected_chunks,
                    bytes: chunk.data.len(),
                    chunks: 1,
                    crc_ok,
                });
                self.finish_chunk()?;
            }
            Payload::Chunk(chunk) => {
                let crc_ok = self.chunk(&chunk)?;
                let current = self
                    .current
                    .as_mut()
                    .ok_or_else(|| Status::invalid_argument("artifact chunk has no begin"))?;
                current.bytes = current
                    .bytes
                    .checked_add(chunk.data.len())
                    .ok_or_else(|| Status::resource_exhausted("artifact size overflow"))?;
                current.chunks += 1;
                current.crc_ok &= crc_ok;
                self.finish_chunk()?;
            }
        }
        Ok(())
    }

    fn finish_chunk(&mut self) -> Result<(), Status> {
        let current = self.current.as_ref().unwrap();
        if current.bytes > current.expected_bytes || current.chunks > current.expected_chunks {
            return Err(Status::invalid_argument(
                "artifact exceeds declared dimensions",
            ));
        }
        if current.chunks == current.expected_chunks {
            if current.bytes != current.expected_bytes {
                return Err(Status::invalid_argument(
                    "artifact size differs from declaration",
                ));
            }
            let current = self.current.take().unwrap();
            self.complete(current.name, current.bytes, current.crc_ok)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn payload() -> FixturePayload {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![7, 11, 13]))],
        )
        .unwrap();
        let mut bytes = vec![];
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        FixturePayload {
            schema: proto::DataType {
                kind: Some(proto::data_type::Kind::Struct(proto::data_type::Struct {
                    fields: vec![proto::data_type::StructField {
                        name: "id".into(),
                        nullable: false,
                        data_type: Some(proto::DataType {
                            kind: Some(proto::data_type::Kind::Long(Default::default())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
            },
            ipc: proto::execute_plan_response::ArrowBatch {
                row_count: 3,
                data: bytes.into(),
                ..Default::default()
            },
        }
    }

    fn fixture() -> Fixture {
        Fixture::new(HashMap::from([("range".into(), payload())])).unwrap()
    }
    fn user() -> Option<proto::UserContext> {
        Some(proto::UserContext {
            user_id: "fixture-test".into(),
            ..Default::default()
        })
    }
    fn execute(id: &str, operation: &str, tags: Vec<String>) -> proto::ExecutePlanRequest {
        proto::ExecutePlanRequest {
            session_id: id.into(),
            user_context: user(),
            operation_id: Some(operation.into()),
            plan: Some(proto::Plan {
                op_type: Some(proto::plan::OpType::Root(proto::Relation {
                    rel_type: Some(proto::relation::RelType::Range(proto::Range {
                        end: 1_000_000_000,
                        step: 1,
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            }),
            tags,
            ..Default::default()
        }
    }
    fn config(
        id: &str,
        operation: proto::config_request::operation::OpType,
    ) -> proto::ConfigRequest {
        proto::ConfigRequest {
            session_id: id.into(),
            user_context: user(),
            operation: Some(proto::config_request::Operation {
                op_type: Some(operation),
            }),
            ..Default::default()
        }
    }
    fn get_timezone(id: &str) -> proto::ConfigRequest {
        config(
            id,
            proto::config_request::operation::OpType::Get(proto::config_request::Get {
                keys: vec!["spark.sql.session.timeZone".into()],
            }),
        )
    }

    #[tokio::test]
    async fn stream_replays_prebuilt_bytes_and_release_until_honors_cursor() {
        let fixture = fixture();
        let id = Uuid::new_v4().to_string();
        let operation = Uuid::new_v4().to_string();
        let expected = fixture.payloads["range"].ipc.data.clone();
        let mut stream = fixture
            .execute_plan(Request::new(execute(
                &id,
                &operation,
                vec!["bench.batches=2".into()],
            )))
            .await
            .unwrap()
            .into_inner();
        let header = stream.next().await.unwrap().unwrap();
        assert_eq!(
            header.schema,
            Some(fixture.payloads["range"].schema.clone())
        );
        assert_eq!(header.response_id, response_id(0));
        let first = stream.next().await.unwrap().unwrap();
        let Some(proto::execute_plan_response::ResponseType::ArrowBatch(batch)) =
            first.response_type
        else {
            panic!("batch missing")
        };
        assert_eq!(batch.data, expected);
        assert_eq!(batch.row_count, 3);
        assert_eq!(batch.start_offset, Some(0));
        let mut reattached = fixture
            .reattach_execute(Request::new(proto::ReattachExecuteRequest {
                session_id: id.clone(),
                user_context: user(),
                operation_id: operation.clone(),
                last_response_id: Some(response_id(1)),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let second = reattached.next().await.unwrap().unwrap();
        assert_eq!(second.response_id, response_id(2));
        let Some(proto::execute_plan_response::ResponseType::ArrowBatch(batch)) =
            second.response_type
        else {
            panic!("batch missing")
        };
        assert_eq!(batch.data, expected);
        assert_eq!(batch.start_offset, Some(3));
        assert!(matches!(
            reattached.next().await.unwrap().unwrap().response_type,
            Some(proto::execute_plan_response::ResponseType::ResultComplete(
                _
            ))
        ));
        assert!(reattached.next().await.is_none());
        fixture
            .release_execute(Request::new(proto::ReleaseExecuteRequest {
                session_id: id.clone(),
                user_context: user(),
                operation_id: operation.clone(),
                release: Some(proto::release_execute_request::Release::ReleaseUntil(
                    proto::release_execute_request::ReleaseUntil {
                        response_id: response_id(1),
                    },
                )),
                ..Default::default()
            }))
            .await
            .unwrap();
        let rejected = fixture
            .reattach_execute(Request::new(proto::ReattachExecuteRequest {
                session_id: id.clone(),
                user_context: user(),
                operation_id: operation.clone(),
                ..Default::default()
            }))
            .await;
        assert!(matches!(rejected, Err(status) if status.code() == tonic::Code::NotFound));
        fixture
            .release_execute(Request::new(proto::ReleaseExecuteRequest {
                session_id: id,
                user_context: user(),
                operation_id: operation,
                release: Some(proto::release_execute_request::Release::ReleaseAll(
                    Default::default(),
                )),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(fixture.stats()["retained_operations"], 0);
        assert_eq!(fixture.stats()["live_operations"], 1);
        drop(stream);
        assert_eq!(fixture.stats()["live_operations"], 0);
        assert_eq!(fixture.stats()["query_executions"], 0);
    }

    #[tokio::test]
    async fn held_stream_interrupt_and_session_release_cancel_without_query_work() {
        let fixture = fixture();
        let id = Uuid::new_v4().to_string();
        let operation = Uuid::new_v4().to_string();
        let mut stream = fixture
            .execute_plan(Request::new(execute(
                &id,
                &operation,
                vec!["bench.hold=true".into(), "load".into()],
            )))
            .await
            .unwrap()
            .into_inner();
        assert!(stream.next().await.unwrap().unwrap().schema.is_some());
        let interrupted = fixture
            .interrupt(Request::new(proto::InterruptRequest {
                session_id: id.clone(),
                user_context: user(),
                interrupt_type: 2,
                interrupt: Some(proto::interrupt_request::Interrupt::OperationTag(
                    "load".into(),
                )),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(interrupted.interrupted_ids, vec![operation]);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err()
                .code(),
            tonic::Code::Cancelled
        );
        assert!(stream.next().await.is_none());
        let second = Uuid::new_v4().to_string();
        let mut stream = fixture
            .execute_plan(Request::new(execute(
                &id,
                &second,
                vec!["bench.hold=true".into()],
            )))
            .await
            .unwrap()
            .into_inner();
        assert!(stream.next().await.unwrap().unwrap().schema.is_some());
        fixture
            .release_session(Request::new(proto::ReleaseSessionRequest {
                session_id: id.clone(),
                user_context: user(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::Cancelled
        );
        assert_eq!(
            fixture
                .config(Request::new(get_timezone(&id)))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        fixture
            .release_session(Request::new(proto::ReleaseSessionRequest {
                session_id: id.clone(),
                user_context: user(),
                allow_reconnect: true,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            fixture
                .config(Request::new(get_timezone(&id)))
                .await
                .unwrap()
                .into_inner()
                .pairs[0]
                .value
                .as_deref(),
            Some("UTC")
        );
    }

    #[tokio::test]
    async fn get_all_strips_only_a_requested_prefix_and_filters_other_keys() {
        use proto::config_request::{operation::OpType, GetAll, Set};
        let fixture = fixture();
        let id = Uuid::new_v4().to_string();
        fixture
            .config(Request::new(config(
                &id,
                OpType::Set(Set {
                    pairs: [
                        ("bench.π.one", "first"),
                        ("bench.π.two", "second"),
                        ("bench.other", "outside"),
                    ]
                    .into_iter()
                    .map(|(key, value)| proto::KeyValue {
                        key: key.into(),
                        value: Some(value.into()),
                    })
                    .collect(),
                    silent: None,
                }),
            )))
            .await
            .unwrap();
        let mut results = Vec::new();
        for prefix in [Some("bench.π."), None, Some(""), Some("absent.")] {
            let response = fixture
                .config(Request::new(config(
                    &id,
                    OpType::GetAll(GetAll {
                        prefix: prefix.map(str::to_owned),
                    }),
                )))
                .await
                .unwrap()
                .into_inner();
            results.push(
                response
                    .pairs
                    .into_iter()
                    .map(|pair| (pair.key, pair.value.unwrap()))
                    .collect::<std::collections::BTreeMap<_, _>>(),
            );
        }
        assert_eq!(
            results[0],
            std::collections::BTreeMap::from([
                ("one".into(), "first".into()),
                ("two".into(), "second".into()),
            ])
        );
        assert_eq!(results[1]["bench.π.one"], "first");
        assert_eq!(results[1]["bench.other"], "outside");
        assert_eq!(results[1]["spark.sql.session.timeZone"], "UTC");
        assert_eq!(results[1], results[2]);
        assert!(results[3].is_empty());
    }

    #[tokio::test]
    async fn cloned_configuration_is_isolated_and_observed_identity_is_checked() {
        let fixture = fixture();
        let id = Uuid::new_v4().to_string();
        let cloned = Uuid::new_v4().to_string();
        fixture
            .config(Request::new(config(
                &id,
                proto::config_request::operation::OpType::Set(proto::config_request::Set {
                    pairs: vec![proto::KeyValue {
                        key: "spark.sql.session.timeZone".into(),
                        value: Some("Europe/Paris".into()),
                    }],
                    silent: None,
                }),
            )))
            .await
            .unwrap();
        fixture
            .clone_session(Request::new(proto::CloneSessionRequest {
                session_id: id.clone(),
                user_context: user(),
                new_session_id: Some(cloned.clone()),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            fixture
                .config(Request::new(get_timezone(&cloned)))
                .await
                .unwrap()
                .into_inner()
                .pairs[0]
                .value
                .as_deref(),
            Some("Europe/Paris")
        );
        fixture
            .config(Request::new(config(
                &cloned,
                proto::config_request::operation::OpType::Unset(proto::config_request::Unset {
                    keys: vec!["spark.sql.session.timeZone".into()],
                }),
            )))
            .await
            .unwrap();
        assert_eq!(
            fixture
                .config(Request::new(get_timezone(&id)))
                .await
                .unwrap()
                .into_inner()
                .pairs[0]
                .value
                .as_deref(),
            Some("Europe/Paris")
        );
        let mut request = get_timezone(&id);
        request.client_observed_server_side_session_id = Some(cloned);
        assert_eq!(
            fixture
                .config(Request::new(request))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        let mut request = get_timezone(&id);
        request.session_id = "invalid".into();
        assert_eq!(
            fixture
                .config(Request::new(request))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn artifact_crc_chunk_dimensions_and_staging_are_validated() {
        use proto::add_artifacts_request as artifact;
        fn chunk(bytes: &[u8]) -> artifact::ArtifactChunk {
            artifact::ArtifactChunk {
                data: bytes.to_vec().into(),
                crc: i64::from(crc32fast::hash(bytes)),
            }
        }
        let mut upload = Upload::default();
        upload
            .accept(Some(artifact::Payload::BeginChunk(
                artifact::BeginChunkedArtifact {
                    name: "cache/one".into(),
                    total_bytes: 6,
                    num_chunks: 2,
                    initial_chunk: Some(chunk(b"abc")),
                },
            )))
            .unwrap();
        assert!(upload.staged.is_empty());
        upload
            .accept(Some(artifact::Payload::Chunk(chunk(b"def"))))
            .unwrap();
        assert_eq!(upload.staged["cache/one"], 6);
        let mut invalid = chunk(b"corrupted");
        invalid.crc ^= 1;
        upload
            .accept(Some(artifact::Payload::Batch(artifact::Batch {
                artifacts: vec![artifact::SingleChunkArtifact {
                    name: "cache/bad".into(),
                    data: Some(invalid),
                }],
            })))
            .unwrap();
        assert!(!upload.staged.contains_key("cache/bad"));
        assert!(!upload.summaries.last().unwrap().is_crc_successful);
        assert_eq!(
            upload
                .accept(Some(artifact::Payload::Chunk(chunk(b"extra"))))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let mut malformed = Upload::default();
        assert_eq!(
            malformed
                .accept(Some(artifact::Payload::BeginChunk(
                    artifact::BeginChunkedArtifact {
                        name: "cache/oversize".into(),
                        total_bytes: 2,
                        num_chunks: 1,
                        initial_chunk: Some(chunk(b"abc"))
                    }
                )))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            relative_name("../escape").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn status_is_a_snapshot_of_retained_operation_state() {
        let fixture = fixture();
        let id = Uuid::new_v4().to_string();
        let operation = Uuid::new_v4().to_string();
        let _stream = fixture
            .execute_plan(Request::new(execute(&id, &operation, vec![])))
            .await
            .unwrap();
        let status = fixture
            .get_status(Request::new(proto::GetStatusRequest {
                session_id: id.clone(),
                user_context: user(),
                operation_status: Some(Default::default()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(status.operation_statuses.len(), 1);
        assert_eq!(status.operation_statuses[0].operation_id, operation);
        assert_eq!(status.operation_statuses[0].state, 4);
        fixture.shutdown();
        assert_eq!(fixture.stats()["sessions"], 0);
    }

    #[tokio::test]
    async fn status_preserves_selector_presence_unknown_ids_and_distinct_request_order() {
        let fixture = fixture();
        let id = Uuid::new_v4().to_string();
        let first = Uuid::new_v4().to_string();
        let second = Uuid::new_v4().to_string();
        let unknown = Uuid::new_v4().to_string();
        for operation in [&first, &second] {
            fixture
                .execute_plan(Request::new(execute(&id, operation, vec![])))
                .await
                .unwrap();
        }
        let mut all = vec![(first.clone(), 4), (second.clone(), 4)];
        all.sort();
        for (ids, expected) in [
            (None, vec![]),
            (Some(vec![]), all),
            (Some(vec![first.clone()]), vec![(first.clone(), 4)]),
            (Some(vec![unknown.clone()]), vec![(unknown.clone(), 1)]),
            (
                Some(vec![
                    second.clone(),
                    unknown.clone(),
                    first.clone(),
                    second.clone(),
                ]),
                vec![
                    (second.clone(), 4),
                    (unknown.clone(), 1),
                    (first.clone(), 4),
                ],
            ),
        ] {
            let response = fixture
                .get_status(Request::new(proto::GetStatusRequest {
                    session_id: id.clone(),
                    user_context: user(),
                    operation_status: ids.map(|operation_ids| {
                        proto::get_status_request::OperationStatusRequest {
                            operation_ids,
                            ..Default::default()
                        }
                    }),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.session_id, id);
            assert_eq!(response.server_side_session_id, id);
            assert_eq!(
                response
                    .operation_statuses
                    .into_iter()
                    .map(|status| (status.operation_id, status.state))
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn status_forgets_released_operations_in_explicit_and_all_id_selectors() {
        let fixture = fixture();
        let id = Uuid::new_v4().to_string();
        let released = Uuid::new_v4().to_string();
        let retained = Uuid::new_v4().to_string();
        for operation in [&released, &retained] {
            fixture
                .execute_plan(Request::new(execute(&id, operation, vec![])))
                .await
                .unwrap();
        }
        fixture
            .release_execute(Request::new(proto::ReleaseExecuteRequest {
                session_id: id.clone(),
                user_context: user(),
                operation_id: released.clone(),
                release: Some(proto::release_execute_request::Release::ReleaseAll(
                    Default::default(),
                )),
                ..Default::default()
            }))
            .await
            .unwrap();
        for (operation_ids, expected) in [
            (vec![released.clone()], vec![(released, 1)]),
            (vec![], vec![(retained, 4)]),
        ] {
            let response = fixture
                .get_status(Request::new(proto::GetStatusRequest {
                    session_id: id.clone(),
                    user_context: user(),
                    operation_status: Some(proto::get_status_request::OperationStatusRequest {
                        operation_ids,
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response
                    .operation_statuses
                    .into_iter()
                    .map(|status| (status.operation_id, status.state))
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn status_validates_session_identity_and_does_not_cross_user_or_session_boundaries() {
        let fixture = fixture();
        let id = Uuid::new_v4().to_string();
        let operation = Uuid::new_v4().to_string();
        fixture
            .execute_plan(Request::new(execute(&id, &operation, vec![])))
            .await
            .unwrap();
        let request = proto::GetStatusRequest {
            session_id: id.clone(),
            user_context: user(),
            operation_status: Some(proto::get_status_request::OperationStatusRequest {
                operation_ids: vec![operation.clone()],
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut other_session = request.clone();
        other_session.session_id = Uuid::new_v4().to_string();
        let mut other_user = request.clone();
        other_user.user_context.as_mut().unwrap().user_id = "other-fixture-user".into();
        for request in [other_session, other_user] {
            let response = fixture
                .get_status(Request::new(request))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.operation_statuses.len(), 1);
            assert_eq!(response.operation_statuses[0].operation_id, operation);
            assert_eq!(response.operation_statuses[0].state, 1);
        }
        let mut mismatch = request.clone();
        mismatch.client_observed_server_side_session_id = Some(Uuid::new_v4().to_string());
        let mut missing_user = request.clone();
        missing_user.user_context = None;
        let mut invalid_session = request;
        invalid_session.session_id = "invalid".into();
        for (request, expected) in [
            (mismatch, tonic::Code::FailedPrecondition),
            (missing_user, tonic::Code::InvalidArgument),
            (invalid_session, tonic::Code::InvalidArgument),
        ] {
            assert_eq!(
                fixture
                    .get_status(Request::new(request))
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
        }
    }

    #[test]
    fn invalid_preloaded_payloads_are_rejected_before_serving() {
        let mut invalid = payload();
        invalid.ipc.row_count += 1;
        assert!(Fixture::new(HashMap::from([("range".into(), invalid)])).is_err());
        let mut invalid = payload();
        invalid.ipc.data = b"not Arrow".to_vec().into();
        assert!(Fixture::new(HashMap::from([("range".into(), invalid)])).is_err());
    }
}
