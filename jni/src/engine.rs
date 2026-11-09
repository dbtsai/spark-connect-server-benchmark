//! Typed JNI implementation of the shared engine contract.
//! No Spark Connect RPC handler or request/response envelope crosses this boundary.
mod zero_copy;
#[cfg(test)]
mod zero_copy_tests;

use super::{take_buffer, BridgeBuffer, MAX_BYTES};
use arrow::{array::ffi::FFI_ArrowArray, datatypes::SchemaRef, ipc::reader::StreamReader};
use futures::stream;
use prost::Message;
use spark_connect_proto as proto;
use spark_connect_server::engine::{
    BatchStream, ConfigResult, Engine, EngineSession, PreparedOperation, PreparedQuery,
    SessionContext,
};
use std::{
    collections::HashMap,
    ffi::c_void,
    io::Cursor,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Condvar, Mutex, OnceLock, Weak,
    },
};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tonic::Status;

extern "C" {
    fn eng_task_new(engine: *mut c_void, error: *mut BridgeBuffer) -> *mut c_void;
    fn eng_open_session(
        engine: *mut c_void,
        user: *const u8,
        user_len: usize,
        id: *const u8,
        id_len: usize,
        task: *mut c_void,
        error: *mut BridgeBuffer,
    ) -> *mut c_void;
    fn eng_prepare(
        session: *mut c_void,
        relation: *const u8,
        len: usize,
        task: *mut c_void,
        error: *mut BridgeBuffer,
    ) -> *mut c_void;
    fn eng_schema(query: *mut c_void, out: *mut BridgeBuffer, error: *mut BridgeBuffer) -> i32;
    fn eng_next_arrow(
        query: *mut c_void,
        out: *mut FFI_ArrowArray,
        lease: *mut c_void,
        defer_release: unsafe extern "C" fn(*mut c_void, *mut c_void),
        error: *mut BridgeBuffer,
    ) -> i32;
    fn eng_arrow_owner_free(owner: *mut c_void);
    fn eng_flags(query: *mut c_void, flags: *mut i32, error: *mut BridgeBuffer) -> i32;
    fn eng_config(
        session: *mut c_void,
        opcode: i32,
        fields: *const u8,
        len: usize,
        task: *mut c_void,
        out: *mut BridgeBuffer,
        error: *mut BridgeBuffer,
    ) -> i32;
    fn eng_cancel(task: *mut c_void);
    fn eng_free(handle: *mut c_void);
}

fn engine_error(error: BridgeBuffer) -> Status {
    let bytes = take_buffer(error);
    decode_error(&String::from_utf8_lossy(&bytes))
}
fn decode_error(message: &str) -> Status {
    if let Some((code, text)) = message
        .strip_prefix("JVM_ENGINE_ERROR:")
        .and_then(|text| text.split_once(':'))
    {
        if let Ok(code @ 1..=16) = code.parse::<i32>() {
            return Status::new(tonic::Code::from_i32(code), text.to_owned());
        }
    }
    Status::internal(format!("JVM engine: {message}"))
}

#[derive(Default)]
struct Counts {
    workers: usize,
    exports: usize,
    handles: usize,
    tasks: HashMap<u64, Weak<Task>>,
}
pub(super) struct JvmEngine {
    factory: *mut c_void,
    closed: AtomicBool,
    counts: Mutex<Counts>,
    drained: Condvar,
    permits: Arc<Semaphore>,
}
// C owns JavaVM/global references and attaches a fresh JNIEnv for each call.
unsafe impl Send for JvmEngine {}
unsafe impl Sync for JvmEngine {}
impl Drop for JvmEngine {
    fn drop(&mut self) {
        unsafe { eng_free(self.factory) };
    }
}
impl JvmEngine {
    pub(super) fn new(factory: *mut c_void) -> Arc<Self> {
        Arc::new(Self {
            factory,
            closed: AtomicBool::new(false),
            counts: Mutex::new(Counts::default()),
            drained: Condvar::new(),
            permits: Arc::new(Semaphore::new(64)),
        })
    }
    pub(super) fn shutdown(&self) {
        let tasks = {
            let counts = self
                .counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.closed.store(true, Ordering::SeqCst);
            counts
                .tasks
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        self.permits.close();
        for task in tasks {
            task.cancel();
        }
    }
    pub(super) fn wait_drained(&self) {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while counts.workers != 0 || counts.handles != 0 || counts.exports != 0 {
            counts = self
                .drained
                .wait(counts)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
    async fn task(self: &Arc<Self>) -> Result<TaskGuard, Status> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("Spark engine is stopping"))?;
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let task = Arc::new(Task {
            core: self.clone(),
            id,
            handle: Mutex::new(None),
            cancelled: AtomicBool::new(false),
            permit: Mutex::new(Some(permit)),
        });
        {
            let mut counts = self
                .counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.closed.load(Ordering::SeqCst) {
                return Err(Status::unavailable("Spark engine is stopping"));
            }
            counts.tasks.insert(id, Arc::downgrade(&task));
        }
        Ok(TaskGuard {
            task,
            complete: false,
        })
    }
    async fn blocking<T: Send + 'static>(
        self: &Arc<Self>,
        action: impl FnOnce() -> Result<T, Status> + Send + 'static,
    ) -> Result<T, Status> {
        // Reserve activity before spawning. Dropping a future cancels its TaskGuard;
        // the worker and all global references remain retained until JNI returns.
        let activity = {
            let mut counts = self
                .counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.closed.load(Ordering::SeqCst) {
                return Err(Status::unavailable("Spark engine is stopping"));
            }
            counts.workers += 1;
            Activity(self.clone())
        };
        tokio::task::spawn_blocking(move || {
            let _activity = activity;
            action()
        })
        .await
        .map_err(|e| Status::internal(format!("Spark engine worker failed: {e}")))?
    }
    fn handle(
        self: &Arc<Self>,
        pointer: *mut c_void,
        error: BridgeBuffer,
    ) -> Result<Arc<Handle>, Status> {
        if pointer.is_null() {
            return Err(engine_error(error));
        }
        // A successful C call must not return an owned error buffer.
        let _ = take_buffer(error);
        self.counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .handles += 1;
        Ok(Arc::new(Handle {
            pointer,
            core: self.clone(),
            permit: Mutex::new(None),
        }))
    }
}
struct Activity(Arc<JvmEngine>);
impl Drop for Activity {
    fn drop(&mut self) {
        let mut c = self
            .0
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        c.workers -= 1;
        self.0.drained.notify_all();
    }
}
struct Handle {
    pointer: *mut c_void,
    core: Arc<JvmEngine>,
    // Keep task admission until its final Java reference has been released.
    permit: Mutex<Option<OwnedSemaphorePermit>>,
}
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}
impl Handle {
    fn pointer(&self) -> *mut c_void {
        self.pointer
    }
}
impl Drop for Handle {
    fn drop(&mut self) {
        queue(Cleanup::Free {
            pointer: self.pointer as usize,
            core: self.core.clone(),
            permit: self
                .permit
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        });
    }
}
enum Cleanup {
    Cancel(Arc<Handle>),
    Arrow {
        owner: usize,
        lease: Box<ExportLease>,
    },
    Free {
        pointer: usize,
        core: Arc<JvmEngine>,
        permit: Option<OwnedSemaphorePermit>,
    },
}
fn cleanup_sender() -> &'static Result<mpsc::Sender<Cleanup>, String> {
    static SENDER: OnceLock<Result<mpsc::Sender<Cleanup>, String>> = OnceLock::new();
    SENDER.get_or_init(|| {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("ferrum-engine-cleanup".into())
            .spawn(move || {
                for item in rx {
                    cleanup(item);
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(tx)
    })
}
fn cleanup(item: Cleanup) {
    match item {
        Cleanup::Arrow { owner, lease } => {
            unsafe {
                eng_arrow_owner_free(owner as *mut c_void);
            }
            drop(lease);
        }
        Cleanup::Cancel(handle) => unsafe {
            eng_cancel(handle.pointer());
        },
        Cleanup::Free {
            pointer,
            core,
            permit: _permit,
        } => {
            unsafe {
                eng_free(pointer as *mut c_void);
            }
            let mut counts = core
                .counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            counts.handles -= 1;
            core.drained.notify_all();
        }
    }
}
fn queue(item: Cleanup) {
    match cleanup_sender() {
        Ok(tx) => {
            if let Err(e) = tx.send(item) {
                cleanup(e.0);
            }
        }
        Err(_) => cleanup(item),
    }
}
// Counts exported batches until the *actual* deferred Java allocator close,
// not just until the final Rust Buffer schedules its release callback.
struct ExportLease(Arc<JvmEngine>);
impl ExportLease {
    fn new(core: Arc<JvmEngine>) -> Box<Self> {
        core.counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .exports += 1;
        Box::new(Self(core))
    }
}
impl Drop for ExportLease {
    fn drop(&mut self) {
        let mut counts = self
            .0
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        counts.exports -= 1;
        self.0.drained.notify_all();
    }
}
// C only invokes this after ownership was transferred on a successful export.
// Normally queues Java cleanup instead of closing the allocator on the Buffer
// drop thread. If the cleanup worker is unavailable, queue releases inline.
unsafe extern "C" fn defer_arrow_release(lease: *mut c_void, owner: *mut c_void) {
    let lease = unsafe { Box::from_raw(lease.cast::<ExportLease>()) };
    queue(Cleanup::Arrow {
        owner: owner as usize,
        lease,
    });
}

struct Task {
    core: Arc<JvmEngine>,
    id: u64,
    handle: Mutex<Option<Arc<Handle>>>,
    cancelled: AtomicBool,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
}
impl Task {
    fn initialize(&self) -> Result<Arc<Handle>, Status> {
        let existing = self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let handle = match existing {
            Some(handle) => handle,
            None => {
                if self.cancelled.load(Ordering::SeqCst) {
                    return Err(Status::cancelled("Spark work cancelled"));
                }
                // Never hold the publication mutex across JNI: future drop and
                // native stop must be able to mark cancellation immediately.
                let mut error = BridgeBuffer::default();
                let pointer = unsafe { eng_task_new(self.core.factory, &mut error) };
                let created = self.core.handle(pointer, error)?;
                self.handle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get_or_insert(created)
                    .clone()
            }
        };
        if let Some(permit) = self
            .permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            *handle
                .permit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(permit);
        }
        if self.cancelled.load(Ordering::SeqCst) {
            unsafe {
                eng_cancel(handle.pointer());
            }
            return Err(Status::cancelled("Spark work cancelled"));
        }
        Ok(handle)
    }
    fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            if let Some(handle) = self
                .handle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
            {
                queue(Cleanup::Cancel(handle));
            }
        }
    }
}
impl Drop for Task {
    fn drop(&mut self) {
        self.core
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tasks
            .remove(&self.id);
    }
}
struct TaskGuard {
    task: Arc<Task>,
    complete: bool,
}
impl TaskGuard {
    fn finish(&mut self) {
        self.complete = true;
    }
}
impl Drop for TaskGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.task.cancel();
        }
    }
}

pub(super) struct EngineFactory(pub Arc<JvmEngine>);
impl Engine for EngineFactory {
    fn create_session(&self, context: SessionContext) -> Result<Arc<dyn EngineSession>, Status> {
        Ok(Arc::new(JvmSession {
            core: self.0.clone(),
            context,
            handle: OnceCell::new(),
            catalog: tokio::sync::Mutex::new(()),
        }))
    }
}
struct JvmSession {
    core: Arc<JvmEngine>,
    context: SessionContext,
    handle: OnceCell<Arc<Handle>>,
    catalog: tokio::sync::Mutex<()>,
}
impl JvmSession {
    async fn handle(&self) -> Result<Arc<Handle>, Status> {
        self.handle
            .get_or_try_init(|| async {
                let mut guard = self.core.task().await?;
                let task = guard.task.clone();
                let core = self.core.clone();
                let user = self
                    .context
                    .user_context
                    .as_ref()
                    .map(|u| u.user_id.clone())
                    .unwrap_or_default();
                let id = self.context.session_id.clone();
                let handle = self
                    .core
                    .blocking(move || {
                        let task = task.initialize()?;
                        let mut error = BridgeBuffer::default();
                        let pointer = unsafe {
                            eng_open_session(
                                core.factory,
                                user.as_ptr(),
                                user.len(),
                                id.as_ptr(),
                                id.len(),
                                task.pointer(),
                                &mut error,
                            )
                        };
                        core.handle(pointer, error)
                    })
                    .await?;
                guard.finish();
                Ok(handle)
            })
            .await
            .cloned()
    }
    async fn relation(&self, relation: proto::Relation) -> Result<JvmQuery, Status> {
        let session = self.handle().await?;
        let guard = self.core.task().await?;
        let task = guard.task.clone();
        let core = self.core.clone();
        let bytes = relation.encode_to_vec();
        let (handle, schema, flags) = self
            .core
            .blocking(move || {
                let task = task.initialize()?;
                let mut error = BridgeBuffer::default();
                let pointer = unsafe {
                    eng_prepare(
                        session.pointer(),
                        bytes.as_ptr(),
                        bytes.len(),
                        task.pointer(),
                        &mut error,
                    )
                };
                let query = core.handle(pointer, error)?;
                let bytes =
                    output(|out, error| unsafe { eng_schema(query.pointer(), out, error) })?;
                let reader = ipc(bytes)?;
                let schema = reader.schema();
                let mut flags = 0;
                let mut error = BridgeBuffer::default();
                if unsafe { eng_flags(query.pointer(), &mut flags, &mut error) } < 0 {
                    return Err(engine_error(error));
                }
                Ok((query, schema, flags))
            })
            .await?;
        Ok(JvmQuery {
            core: self.core.clone(),
            handle,
            guard,
            schema,
            flags,
        })
    }
}
fn ipc(bytes: Vec<u8>) -> Result<StreamReader<Cursor<Vec<u8>>>, Status> {
    if bytes.len() > MAX_BYTES {
        return Err(Status::resource_exhausted(
            "Spark Arrow batch exceeds JNI limit",
        ));
    }
    StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| Status::internal(format!("invalid Spark Arrow stream: {e}")))
}
fn output(
    action: impl FnOnce(*mut BridgeBuffer, *mut BridgeBuffer) -> i32,
) -> Result<Vec<u8>, Status> {
    let mut out = BridgeBuffer::default();
    let mut error = BridgeBuffer::default();
    if action(&mut out, &mut error) < 0 {
        let _ = take_buffer(out);
        Err(engine_error(error))
    } else {
        let _ = take_buffer(error);
        Ok(take_buffer(out))
    }
}
struct JvmQuery {
    core: Arc<JvmEngine>,
    handle: Arc<Handle>,
    guard: TaskGuard,
    schema: SchemaRef,
    flags: i32,
}
#[tonic::async_trait]
impl PreparedQuery for JvmQuery {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    async fn execute(self: Box<Self>) -> Result<BatchStream, Status> {
        Ok(Box::pin(stream::try_unfold(
            *self,
            |mut query| async move {
                let handle = query.handle.clone();
                let task = query.guard.task.clone();
                let core = query.core.clone();
                let schema = query.schema.clone();
                let result = query
                    .core
                    .blocking(move || {
                        task.initialize()?;
                        let lease = ExportLease::new(core);
                        let lease_pointer = Box::into_raw(lease);
                        let mut out = FFI_ArrowArray::empty();
                        let mut error = BridgeBuffer::default();
                        let result = unsafe {
                            eng_next_arrow(
                                handle.pointer(),
                                &mut out,
                                lease_pointer.cast(),
                                defer_arrow_release,
                                &mut error,
                            )
                        };
                        if result != 1 {
                            // C transfers the lease only on success. Partial C
                            // descriptors are released by C before returning an error.
                            drop(unsafe { Box::from_raw(lease_pointer) });
                            if result < 0 {
                                return Err(engine_error(error));
                            }
                            let _ = take_buffer(error);
                            return Ok(None);
                        }
                        let _ = take_buffer(error);
                        // The C descriptor now owns the lease through its callback;
                        // errors, panics, early drops, and cloned/sliced buffers all
                        // eventually schedule the same independent Java batch close.
                        let batch = unsafe { zero_copy::import_batch(out, schema) }?;
                        Ok(Some(batch))
                    })
                    .await?;
                match result {
                    Some(batch) => Ok(Some((batch, query))),
                    None => {
                        query.guard.finish();
                        Ok(None)
                    }
                }
            },
        )))
    }
}
fn root(plan: Option<proto::Plan>) -> Result<proto::Relation, Status> {
    match plan.and_then(|p| p.op_type) {
        Some(proto::plan::OpType::Root(r)) => Ok(r),
        _ => Err(Status::invalid_argument(
            "analysis requires a root relation",
        )),
    }
}
#[tonic::async_trait]
impl EngineSession for JvmSession {
    async fn prepare(&self, operation: proto::plan::OpType) -> Result<PreparedOperation, Status> {
        let _catalog = self.catalog.lock().await;
        match operation {
            proto::plan::OpType::Root(relation) => Ok(PreparedOperation::Query(Box::new(
                self.relation(relation).await?,
            ))),
            proto::plan::OpType::Command(command) => match command.command_type {
                Some(proto::command::CommandType::SqlCommand(sql)) => {
                    #[allow(deprecated)]
                    if !sql.args.is_empty()
                        || !sql.pos_args.is_empty()
                        || !sql.named_arguments.is_empty()
                        || !sql.pos_arguments.is_empty()
                    {
                        return Err(Status::unimplemented("Spark engine SQL command arguments"));
                    }
                    #[allow(deprecated)]
                    let relation = match sql.input {
                        Some(r) => r,
                        None if !sql.sql.is_empty() => proto::Relation {
                            rel_type: Some(proto::relation::RelType::Sql(proto::Sql {
                                query: sql.sql,
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                        _ => return Err(Status::invalid_argument("missing SQL input")),
                    };
                    // Validate as a read-only query before returning the relation.
                    let _query = self.relation(relation.clone()).await?;
                    Ok(PreparedOperation::Command(vec![
                        proto::execute_plan_response::ResponseType::SqlCommandResult(
                            proto::execute_plan_response::SqlCommandResult {
                                relation: Some(relation),
                            },
                        ),
                    ]))
                }
                _ => Err(Status::unimplemented(
                    "Spark engine command is not implemented",
                )),
            },
            _ => Err(Status::unimplemented("compressed Spark engine plans")),
        }
    }
    async fn analyze(
        &self,
        input: proto::analyze_plan_request::Analyze,
    ) -> Result<proto::analyze_plan_response::Result, Status> {
        use proto::{analyze_plan_request::Analyze, analyze_plan_response as out};
        let _catalog = self.catalog.lock().await;
        Ok(match input {
            Analyze::Schema(input) => {
                let query = self.relation(root(input.plan)?).await?;
                out::Result::Schema(out::Schema {
                    schema: Some(spark_connect_server::engine::spark_schema(
                        query.schema.as_ref(),
                    )?),
                })
            }
            Analyze::IsLocal(input) => {
                let query = self.relation(root(input.plan)?).await?;
                out::Result::IsLocal(out::IsLocal {
                    is_local: query.flags & 1 != 0,
                })
            }
            Analyze::IsStreaming(input) => {
                let query = self.relation(root(input.plan)?).await?;
                out::Result::IsStreaming(out::IsStreaming {
                    is_streaming: query.flags & 2 != 0,
                })
            }
            _ => {
                return Err(Status::unimplemented(
                    "Spark engine analysis is not implemented",
                ))
            }
        })
    }
    async fn config(
        &self,
        input: proto::config_request::operation::OpType,
    ) -> Result<ConfigResult, Status> {
        use proto::config_request::operation::OpType;
        let _catalog = self.catalog.lock().await;
        let keys = |items: Vec<String>| {
            items
                .into_iter()
                .map(|key| proto::KeyValue { key, value: None })
                .collect::<Vec<_>>()
        };
        let (opcode, pairs) = match input {
            OpType::Get(v) => (1, keys(v.keys)),
            OpType::GetOption(v) => (2, keys(v.keys)),
            OpType::GetWithDefault(v) => (3, v.pairs),
            OpType::GetAll(v) => (4, v.prefix.map(|p| keys(vec![p])).unwrap_or_default()),
            OpType::IsModifiable(v) => (5, keys(v.keys)),
            OpType::Set(v) => {
                if v.silent == Some(true) {
                    return Err(Status::unimplemented("silent Spark engine configuration"));
                }
                (6, v.pairs)
            }
            OpType::Unset(v) => (7, keys(v.keys)),
        };
        let bytes = encode_pairs(&pairs)?;
        let session = self.handle().await?;
        let mut guard = self.core.task().await?;
        let task = guard.task.clone();
        let bytes = self
            .core
            .blocking(move || {
                let task = task.initialize()?;
                output(|out, error| unsafe {
                    eng_config(
                        session.pointer(),
                        opcode,
                        bytes.as_ptr(),
                        bytes.len(),
                        task.pointer(),
                        out,
                        error,
                    )
                })
            })
            .await?;
        guard.finish();
        Ok(ConfigResult {
            pairs: decode_pairs(&bytes)?,
            warnings: Vec::new(),
        })
    }
}
fn encode_pairs(pairs: &[proto::KeyValue]) -> Result<Vec<u8>, Status> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &u32::try_from(pairs.len())
            .map_err(|_| Status::resource_exhausted("too many config fields"))?
            .to_be_bytes(),
    );
    for pair in pairs {
        super::put_bytes(&mut bytes, pair.key.as_bytes())?;
        if let Some(value) = &pair.value {
            let len = i32::try_from(value.len())
                .map_err(|_| Status::resource_exhausted("config value too large"))?;
            bytes.extend_from_slice(&len.to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        } else {
            bytes.extend_from_slice(&(-1_i32).to_be_bytes());
        }
    }
    Ok(bytes)
}
fn decode_pairs(bytes: &[u8]) -> Result<Vec<proto::KeyValue>, Status> {
    let mut cursor = super::Cursor(bytes);
    let count = cursor.integer()? as usize;
    if count > cursor.0.len() / 8 {
        return Err(Status::internal("invalid Spark config pair count"));
    }
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        let key = String::from_utf8(cursor.bytes()?.to_vec())
            .map_err(|_| Status::internal("invalid Spark config UTF8"))?;
        let len = cursor.integer()? as i32;
        let value = if len == -1 {
            None
        } else if len < 0 {
            return Err(Status::internal("invalid Spark config value length"));
        } else {
            let len = len as usize;
            let field = cursor
                .0
                .get(..len)
                .ok_or_else(|| Status::internal("truncated Spark config value"))?;
            cursor.0 = &cursor.0[len..];
            Some(
                String::from_utf8(field.to_vec())
                    .map_err(|_| Status::internal("invalid Spark config UTF8"))?,
            )
        };
        result.push(proto::KeyValue { key, value });
    }
    if !cursor.0.is_empty() {
        return Err(Status::internal("trailing Spark config data"));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shutdown_waits_for_exported_buffers_to_be_released() {
        let core = JvmEngine::new(std::ptr::null_mut());
        let lease = ExportLease::new(core.clone());
        core.shutdown();
        let (started_tx, started_rx) = mpsc::channel();
        let (drained_tx, drained_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            core.wait_drained();
            drained_tx.send(()).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(drained_rx
            .recv_timeout(std::time::Duration::from_millis(20))
            .is_err());
        // A real final-buffer callback queues this pair. Null owner exercises
        // that path without a JVM; C treats it as an empty cleanup operation.
        queue(Cleanup::Arrow { owner: 0, lease });
        drained_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        waiter.join().unwrap();
    }
    #[test]
    fn typed_errors_preserve_codes_and_reject_ok_or_invalid_codes() {
        assert_eq!(
            decode_error("JVM_ENGINE_ERROR:12:unsupported: feature").code(),
            tonic::Code::Unimplemented
        );
        assert_eq!(
            decode_error("JVM_ENGINE_ERROR:1:cancelled 😀").message(),
            "cancelled 😀"
        );
        for text in [
            "JVM_ENGINE_ERROR:0:bad",
            "JVM_ENGINE_ERROR:99:bad",
            "plain failure",
        ] {
            assert_eq!(decode_error(text).code(), tonic::Code::Internal);
        }
    }
    #[tokio::test]
    async fn shutdown_cancels_pending_tokens_before_their_first_jni_call() {
        // No JNI call occurs: cancellation is observed before task publication.
        let core = JvmEngine::new(std::ptr::null_mut());
        let guard = core.task().await.unwrap();
        core.shutdown();
        assert!(
            matches!(guard.task.initialize(), Err(status) if status.code() == tonic::Code::Cancelled)
        );
        assert!(core.task().await.is_err());
        drop(guard);
        core.wait_drained();
    }
    #[test]
    fn config_codec_preserves_null_unicode_and_empty() {
        let pairs = vec![
            proto::KeyValue {
                key: "时区".into(),
                value: Some("😀".into()),
            },
            proto::KeyValue {
                key: "".into(),
                value: None,
            },
            proto::KeyValue {
                key: "empty".into(),
                value: Some("".into()),
            },
        ];
        assert_eq!(pairs, decode_pairs(&encode_pairs(&pairs).unwrap()).unwrap());
    }
    #[test]
    fn config_codec_rejects_trailing_and_invalid_lengths() {
        assert!(decode_pairs(&[0, 0, 0, 0, 1]).is_err());
        assert!(decode_pairs(&[0, 0, 0, 1, 0, 0, 0, 0, 255, 255, 255, 254]).is_err());
    }
}
