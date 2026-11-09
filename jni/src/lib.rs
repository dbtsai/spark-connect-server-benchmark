//! JVM-loaded JNI host. Java objects are retained by the C shim, never by JNIEnv pointers.

mod engine;

use std::{
    collections::HashMap,
    ffi::c_void,
    net::SocketAddr,
    pin::Pin,
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        mpsc as std_mpsc, Arc, Condvar, Mutex, OnceLock, Weak,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::{Stream, StreamExt};
use spark_connect_server::{
    embedded::{RpcBackend, RpcStream},
    Backend,
};
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tonic::{
    codegen::Bytes,
    metadata::{KeyAndValueRef, MetadataMap},
    Code, Request, Response, Status,
};

const MAX_BYTES: usize = 64 * 1024 * 1024;
// Each call uses at most two blocking workers. Leave capacity for polls even
// when every synchronous Spark input handler is blocked on response backpressure.
const MAX_CALLS: usize = 64;

#[repr(C)]
pub struct BridgeBuffer {
    data: *mut u8,
    len: usize,
}
impl Default for BridgeBuffer {
    fn default() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
        }
    }
}

extern "C" {
    fn bridge_open(
        bridge: *mut c_void,
        method: *const u8,
        method_len: usize,
        metadata: *const u8,
        metadata_len: usize,
        timeout_ns: i64,
        error: *mut BridgeBuffer,
    ) -> *mut c_void;
    fn bridge_send(call: *mut c_void, data: *const u8, len: usize, error: *mut BridgeBuffer)
        -> i32;
    fn bridge_half_close(call: *mut c_void, error: *mut BridgeBuffer) -> i32;
    fn bridge_poll(
        call: *mut c_void,
        timeout_ms: i64,
        out: *mut BridgeBuffer,
        error: *mut BridgeBuffer,
    ) -> i32;
    fn bridge_cancel(call: *mut c_void);
    fn bridge_finish(call: *mut c_void);
    fn bridge_call_free(call: *mut c_void);
    fn bridge_free(bridge: *mut c_void);
    fn bridge_buffer_free(buffer: *mut BridgeBuffer);
    fn bridge_native_start(
        env: *mut c_void,
        class: *mut c_void,
        address: *mut c_void,
        dispatcher: *mut c_void,
    ) -> i64;
    fn bridge_native_address(env: *mut c_void, class: *mut c_void, id: i64) -> *mut c_void;
    fn bridge_native_stop(env: *mut c_void, class: *mut c_void, id: i64);
}

fn take_buffer(mut buffer: BridgeBuffer) -> Vec<u8> {
    let result = if buffer.data.is_null() || buffer.len == 0 {
        Vec::new()
    } else {
        // C returns malloc-owned buffers valid until bridge_buffer_free.
        unsafe { std::slice::from_raw_parts(buffer.data, buffer.len).to_vec() }
    };
    unsafe {
        bridge_buffer_free(&mut buffer);
    }
    result
}

fn java_error(error: BridgeBuffer) -> Status {
    Status::internal(format!(
        "JVM bridge: {}",
        String::from_utf8_lossy(&take_buffer(error))
    ))
}

struct Bridge(*mut c_void);
// The shim stores JavaVM/global references and attaches a separate JNIEnv per operation.
unsafe impl Send for Bridge {}
unsafe impl Sync for Bridge {}
impl Drop for Bridge {
    fn drop(&mut self) {
        unsafe {
            bridge_free(self.0);
        }
    }
}

struct State {
    closed: AtomicBool,
    calls: Mutex<HashMap<u64, Weak<JavaCall>>>,
    active: Mutex<usize>,
    drained: Condvar,
}

impl State {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            calls: Mutex::new(HashMap::new()),
            active: Mutex::new(0),
            drained: Condvar::new(),
        }
    }
    fn wait_drained(&self) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *active != 0 {
            active = self
                .drained
                .wait(active)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

struct Activity {
    state: Arc<State>,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Activity {
    fn drop(&mut self) {
        let mut active = self
            .state
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active -= 1;
        self.state.drained.notify_all();
    }
}

struct JavaCall {
    pointer: *mut c_void,
    bridge: Arc<Bridge>,
    id: u64,
    stopped: watch::Sender<bool>,
    cancellation_queued: AtomicBool,
    completed: AtomicBool,
    failure: Mutex<Option<Status>>,
    activity: Option<Activity>,
}
// Java Call serializes listener callbacks; polling and cancellation have independent locks.
unsafe impl Send for JavaCall {}
unsafe impl Sync for JavaCall {}

impl JavaCall {
    fn fail(self: &Arc<Self>, status: Status) {
        if !self.completed.load(Ordering::SeqCst) {
            self.failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert(status);
            self.cancel();
        }
    }
    fn cancel(self: &Arc<Self>) {
        self.stopped.send_replace(true);
        if !self.completed.load(Ordering::SeqCst)
            && !self.cancellation_queued.swap(true, Ordering::SeqCst)
        {
            enqueue(Cleanup::Cancel(self.clone()));
        }
    }
}

struct Finalize {
    pointer: *mut c_void,
    _bridge: Arc<Bridge>,
    activity: Activity,
    id: u64,
}
unsafe impl Send for Finalize {}
enum Cleanup {
    Cancel(Arc<JavaCall>),
    Finalize(Finalize),
}

// The native library is process-lived. This worker performs JNI cleanup away from
// Tokio workers and remains idle between servers; library unloading is unsupported.
fn cleanup() -> &'static Result<std_mpsc::Sender<Cleanup>, String> {
    static CLEANUP: OnceLock<Result<std_mpsc::Sender<Cleanup>, String>> = OnceLock::new();
    CLEANUP.get_or_init(|| {
        let (sender, receiver) = std_mpsc::channel();
        std::thread::Builder::new()
            .name("ferrum-jni-cleanup".into())
            .spawn(move || {
                for job in receiver {
                    run_cleanup(job);
                }
            })
            .map_err(|error| format!("create JNI cleanup worker: {error}"))?;
        Ok(sender)
    })
}

fn run_cleanup(job: Cleanup) {
    match job {
        Cleanup::Cancel(call) => unsafe {
            bridge_cancel(call.pointer);
        },
        Cleanup::Finalize(call) => {
            // Java finish cancels unfinished calls and completes consumed calls.
            unsafe {
                bridge_finish(call.pointer);
                bridge_call_free(call.pointer);
            }
            call.activity
                .state
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&call.id);
        }
    }
}

fn enqueue(job: Cleanup) {
    match cleanup() {
        Ok(sender) => {
            if let Err(error) = sender.send(job) {
                run_cleanup(error.0);
            }
        }
        // Startup initializes the worker before admitting calls. If it ever
        // becomes unavailable, release JNI ownership synchronously, never panic in Drop.
        Err(_) => run_cleanup(job),
    }
}

impl Drop for JavaCall {
    fn drop(&mut self) {
        if let Some(activity) = self.activity.take() {
            enqueue(Cleanup::Finalize(Finalize {
                pointer: self.pointer,
                _bridge: self.bridge.clone(),
                activity,
                id: self.id,
            }));
        }
    }
}

struct DirectBackend {
    bridge: Arc<Bridge>,
    state: Arc<State>,
    capacity: Arc<Semaphore>,
}

impl DirectBackend {
    fn new(pointer: *mut c_void) -> Self {
        Self {
            bridge: Arc::new(Bridge(pointer)),
            state: Arc::new(State::new()),
            capacity: Arc::new(Semaphore::new(MAX_CALLS)),
        }
    }
    fn reserve(&self) -> Result<Activity, Status> {
        let _calls = self
            .state
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.state.closed.load(Ordering::SeqCst) {
            return Err(Status::unavailable("embedded backend is shutting down"));
        }
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("embedded JVM call limit reached (64)"))?;
        *self
            .state
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        Ok(Activity {
            state: self.state.clone(),
            _permit: permit,
        })
    }
    fn open(
        &self,
        method: String,
        metadata: Vec<u8>,
        timeout_ns: i64,
        activity: Activity,
    ) -> Result<Arc<JavaCall>, Status> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        // fetch_update keeps the Rust 1.88 MSRV; its new name requires Rust 1.95.
        #[allow(deprecated)]
        let id = NEXT
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |id| id.checked_add(1))
            .map_err(|_| Status::resource_exhausted("JNI call IDs exhausted"))?;
        if self.state.closed.load(Ordering::SeqCst) {
            return Err(Status::unavailable("embedded backend is shutting down"));
        }
        let mut error = BridgeBuffer::default();
        let pointer = unsafe {
            bridge_open(
                self.bridge.0,
                method.as_ptr(),
                method.len(),
                metadata.as_ptr(),
                metadata.len(),
                timeout_ns,
                &mut error,
            )
        };
        if pointer.is_null() {
            return Err(java_error(error));
        }
        let call = Arc::new(JavaCall {
            pointer,
            bridge: self.bridge.clone(),
            id,
            stopped: watch::channel(false).0,
            cancellation_queued: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            failure: Mutex::new(None),
            activity: Some(activity),
        });
        {
            let mut calls = self
                .state
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            calls.insert(id, Arc::downgrade(&call));
        }
        if self.state.closed.load(Ordering::SeqCst) {
            call.cancel();
            return Err(Status::unavailable("embedded backend is shutting down"));
        }
        Ok(call)
    }
}

struct CallGuard(Weak<JavaCall>);
impl Drop for CallGuard {
    fn drop(&mut self) {
        if let Some(call) = self.0.upgrade() {
            call.cancel();
        }
    }
}
struct Responses {
    receiver: mpsc::Receiver<Result<Bytes, Status>>,
    terminal: Arc<Mutex<Option<Status>>>,
    _guard: CallGuard,
}
impl Stream for Responses {
    type Item = Result<Bytes, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(None) => Poll::Ready(
                self.terminal
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .map(Err),
            ),
            result => result,
        }
    }
}

async fn stopped(mut receiver: watch::Receiver<bool>) {
    if *receiver.borrow() {
        return;
    }
    let _ = receiver.changed().await;
}

#[allow(clippy::double_must_use)]
#[tonic::async_trait]
impl RpcBackend for DirectBackend {
    async fn call(
        &self,
        method: String,
        request: Request<RpcStream>,
    ) -> Result<Response<RpcStream>, Status> {
        let metadata = encode_metadata(request.metadata())?;
        let timeout_ns = timeout_nanos(request.metadata())?;
        let bridge = self.bridge.clone();
        let state = self.state.clone();
        let capacity = self.capacity.clone();
        let activity = self.reserve()?;
        let opening = tokio::task::spawn_blocking(move || {
            DirectBackend {
                bridge,
                state,
                capacity,
            }
            .open(method, metadata, timeout_ns, activity)
        });
        let call = opening
            .await
            .map_err(|_| Status::internal("JNI open task failed"))??;
        let guard = CallGuard(Arc::downgrade(&call));
        let (headers, head) = oneshot::channel();
        let (output, receiver) = mpsc::channel(2);
        let terminal = Arc::new(Mutex::new(None));
        let output_terminal = terminal.clone();
        let mut input = request.into_inner();
        let input_call = call.clone();
        tokio::spawn(async move {
            loop {
                let next = tokio::select! { biased; _ = stopped(input_call.stopped.subscribe()) => return, next = input.next() => next };
                match next {
                    Some(Ok(bytes)) => {
                        let worker = input_call.clone();
                        let sent = tokio::task::spawn_blocking(move || {
                            let mut error = BridgeBuffer::default();
                            let result = unsafe {
                                bridge_send(worker.pointer, bytes.as_ptr(), bytes.len(), &mut error)
                            };
                            if result < 0 {
                                Err(java_error(error))
                            } else {
                                Ok(result != 0)
                            }
                        })
                        .await;
                        match sent {
                            Ok(Ok(true)) => {}
                            // Spark may already have sent its terminal status. Let the
                            // output pump consume it, without replacing it with cancellation.
                            Ok(Ok(false)) => return,
                            Ok(Err(error)) => {
                                input_call.fail(error);
                                return;
                            }
                            Err(_) => {
                                input_call.fail(Status::internal("JNI send task failed"));
                                return;
                            }
                        }
                    }
                    Some(Err(error)) => {
                        input_call.fail(error);
                        return;
                    }
                    None => {
                        let worker = input_call.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            let mut error = BridgeBuffer::default();
                            let code = unsafe { bridge_half_close(worker.pointer, &mut error) };
                            if code < 0 {
                                Err(java_error(error))
                            } else {
                                Ok(())
                            }
                        })
                        .await;
                        match result {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => input_call.fail(error),
                            Err(_) => {
                                input_call.fail(Status::internal("JNI half-close task failed"))
                            }
                        }
                        return;
                    }
                }
            }
        });
        let output_call = call.clone();
        tokio::spawn(async move {
            let mut headers = Some(headers);
            let mut final_status = None;
            loop {
                let worker = output_call.clone();
                let event = tokio::select! {
                    biased;
                    _ = stopped(output_call.stopped.subscribe()) => break,
                    _ = output.closed() => { output_call.cancel(); break; },
                    event = tokio::task::spawn_blocking(move || {
                        let mut event = BridgeBuffer::default(); let mut error = BridgeBuffer::default();
                        match unsafe { bridge_poll(worker.pointer, 100, &mut event, &mut error) } {
                            0 => Ok(None), 1 => decode_event(take_buffer(event)).map(Some), _ => Err(java_error(error)),
                        }
                    }) => event.unwrap_or_else(|_| Err(Status::internal("JNI poll task failed"))),
                };
                let event = match event {
                    Ok(None) => continue,
                    Ok(Some(event)) => event,
                    Err(error) => {
                        output_call.fail(error);
                        break;
                    }
                };
                match event {
                    Event::Headers(metadata) => {
                        if let Some(headers) = headers.take() {
                            let _ = headers.send(Ok(metadata));
                        }
                    }
                    Event::Message(bytes) => {
                        if let Some(headers) = headers.take() {
                            let _ = headers.send(Ok(MetadataMap::new()));
                        }
                        tokio::select! { biased; _ = stopped(output_call.stopped.subscribe()) => break, result = output.send(Ok(bytes)) => if result.is_err() { output_call.cancel(); break; } }
                    }
                    Event::Close(status) => {
                        output_call.completed.store(true, Ordering::SeqCst);
                        output_call.stopped.send_replace(true);
                        final_status = Some(status);
                        break;
                    }
                }
            }
            let status = final_status
                .or_else(|| {
                    output_call
                        .failure
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                })
                .unwrap_or_else(|| Status::cancelled("embedded call cancelled"));
            if let Some(headers) = headers.take() {
                let _ = headers.send(Err(status));
            } else {
                *output_terminal
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(status);
            }
        });
        drop(call);
        let metadata = head
            .await
            .map_err(|_| Status::cancelled("embedded call closed before headers"))??;
        let stream = Box::pin(Responses {
            receiver,
            terminal,
            _guard: guard,
        }) as RpcStream;
        let mut response = Response::new(stream);
        *response.metadata_mut() = metadata;
        Ok(response)
    }

    fn shutdown(&self) {
        let calls: Vec<_> = {
            let calls = self
                .state
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.state.closed.store(true, Ordering::SeqCst);
            calls.values().filter_map(Weak::upgrade).collect()
        };
        for call in calls {
            call.cancel();
        }
    }
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), Status> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| Status::resource_exhausted("JNI field too large"))?;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}
fn encode_metadata(metadata: &MetadataMap) -> Result<Vec<u8>, Status> {
    let mut pairs = Vec::new();
    for item in metadata.iter() {
        match item {
            KeyAndValueRef::Ascii(key, value) => {
                pairs.push((key.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
            }
            KeyAndValueRef::Binary(key, value) => pairs.push((
                key.as_str().as_bytes().to_vec(),
                value
                    .to_bytes()
                    .map_err(|_| Status::invalid_argument("invalid binary metadata"))?
                    .to_vec(),
            )),
        }
    }
    let mut output = Vec::new();
    output.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
    for (key, value) in pairs {
        put_bytes(&mut output, &key)?;
        put_bytes(&mut output, &value)?;
    }
    Ok(output)
}
struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn integer(&mut self) -> Result<u32, Status> {
        let bytes = self
            .0
            .get(..4)
            .ok_or_else(|| Status::internal("truncated JNI event"))?;
        let value = u32::from_be_bytes(bytes.try_into().unwrap());
        self.0 = &self.0[4..];
        Ok(value)
    }
    fn bytes(&mut self) -> Result<&'a [u8], Status> {
        let length = self.integer()? as usize;
        let bytes = self
            .0
            .get(..length)
            .ok_or_else(|| Status::internal("truncated JNI field"))?;
        self.0 = &self.0[length..];
        Ok(bytes)
    }
    fn metadata(&mut self) -> Result<MetadataMap, Status> {
        let count = self.integer()?;
        let mut metadata = MetadataMap::new();
        if count as usize > self.0.len() / 8 {
            return Err(Status::internal("invalid JNI metadata count"));
        }
        for _ in 0..count {
            let key = self.bytes()?;
            let value = self.bytes()?;
            if key.ends_with(b"-bin") {
                let key = tonic::metadata::MetadataKey::<tonic::metadata::Binary>::from_bytes(key)
                    .map_err(|_| Status::internal("invalid binary metadata key"))?;
                metadata.append_bin(key, tonic::metadata::MetadataValue::from_bytes(value));
            } else {
                let key = tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(key)
                    .map_err(|_| Status::internal("invalid ASCII metadata key"))?;
                let value = tonic::metadata::MetadataValue::try_from(value)
                    .map_err(|_| Status::internal("invalid ASCII metadata value"))?;
                metadata.append(key, value);
            }
        }
        Ok(metadata)
    }
}
enum Event {
    Headers(MetadataMap),
    Message(Bytes),
    Close(Status),
}
fn decode_event(bytes: Vec<u8>) -> Result<Event, Status> {
    if bytes.len() > MAX_BYTES + 1024 * 1024 {
        return Err(Status::resource_exhausted("JNI response event too large"));
    }
    let (&kind, rest) = bytes
        .split_first()
        .ok_or_else(|| Status::internal("empty JNI event"))?;
    let mut cursor = Cursor(rest);
    let event = match kind {
        1 => Event::Headers(cursor.metadata()?),
        2 => {
            let payload = Bytes::copy_from_slice(cursor.0);
            cursor.0 = &[];
            Event::Message(payload)
        }
        3 => {
            let code = cursor.integer()?;
            let description = String::from_utf8(cursor.bytes()?.to_vec())
                .map_err(|_| Status::internal("invalid JNI status UTF-8"))?;
            let mut metadata = cursor.metadata()?;
            // Tonic sanitizes reserved metadata names when serializing a Status.
            // Carry structured Spark errors through its dedicated details field.
            let details = metadata
                .remove_bin("grpc-status-details-bin")
                .map(|value| {
                    value
                        .to_bytes()
                        .map_err(|_| Status::internal("invalid JNI status details"))
                })
                .transpose()?
                .unwrap_or_default();
            Event::Close(Status::with_details_and_metadata(
                Code::from_i32(code as i32),
                description,
                details,
                metadata,
            ))
        }
        _ => return Err(Status::internal("unknown JNI event")),
    };
    if !cursor.0.is_empty() {
        return Err(Status::internal("trailing JNI event bytes"));
    }
    Ok(event)
}
fn timeout_nanos(metadata: &MetadataMap) -> Result<i64, Status> {
    let Some(value) = metadata.get("grpc-timeout") else {
        return Ok(-1);
    };
    let value = value
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout"))?;
    if value.len() < 2 || value.len() > 9 {
        return Err(Status::invalid_argument("invalid grpc-timeout"));
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.bytes().all(|digit| digit.is_ascii_digit()) {
        return Err(Status::invalid_argument("invalid grpc-timeout"));
    }
    let amount: u64 = digits
        .parse()
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout"))?;
    let multiplier = match unit {
        "H" => 3_600_000_000_000_u64,
        "M" => 60_000_000_000,
        "S" => 1_000_000_000,
        "m" => 1_000_000,
        "u" => 1_000,
        "n" => 1,
        _ => return Err(Status::invalid_argument("invalid grpc-timeout")),
    };
    Ok(amount.saturating_mul(multiplier).min(i64::MAX as u64) as i64)
}

struct ServerEntry {
    address: String,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    done: Mutex<std_mpsc::Receiver<Result<(), String>>>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    stopping: Mutex<()>,
    drained: AtomicBool,
    backend: HostedBackend,
}
fn servers() -> &'static Mutex<HashMap<i64, Arc<ServerEntry>>> {
    static SERVERS: OnceLock<Mutex<HashMap<i64, Arc<ServerEntry>>>> = OnceLock::new();
    SERVERS.get_or_init(Mutex::default)
}

#[derive(Clone)]
enum HostedBackend {
    Rpc(Arc<DirectBackend>),
    Engine(Arc<engine::JvmEngine>),
}
impl HostedBackend {
    fn shutdown(&self) {
        match self {
            Self::Rpc(backend) => backend.shutdown(),
            Self::Engine(backend) => backend.shutdown(),
        }
    }
    fn wait_drained(&self) {
        match self {
            Self::Rpc(backend) => backend.state.wait_drained(),
            Self::Engine(backend) => backend.wait_drained(),
        }
    }
    fn server_backend(&self) -> Backend {
        match self {
            Self::Rpc(backend) => Backend::Embedded(backend.clone()),
            // The factory implementation retains the same engine state as the host.
            Self::Engine(backend) => {
                Backend::Native(Arc::new(engine::EngineFactory(backend.clone())))
            }
        }
    }
}
fn start_server(address: &[u8], bridge: *mut c_void) -> Result<i64, String> {
    start_host(
        address,
        HostedBackend::Rpc(Arc::new(DirectBackend::new(bridge))),
    )
}
fn start_host(address: &[u8], backend: HostedBackend) -> Result<i64, String> {
    cleanup().as_ref().map_err(Clone::clone)?;
    static NEXT: AtomicI64 = AtomicI64::new(1);
    #[allow(deprecated)] // Renamed only in Rust 1.95; preserve the declared MSRV.
    let id = NEXT
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |id| id.checked_add(1))
        .map_err(|_| "JNI server IDs exhausted")?;
    let address: SocketAddr = std::str::from_utf8(address)
        .map_err(|_| "listen address must be UTF-8")?
        .parse()
        .map_err(|_| "listen address must be an IP:port")?;
    let listener = std::net::TcpListener::bind(address).map_err(|e| e.to_string())?;
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    let address = listener
        .local_addr()
        .map_err(|e| e.to_string())?
        .to_string();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(256)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    let (shutdown, stopped) = oneshot::channel();
    let (done, completed) = std_mpsc::channel();
    let worker_backend = backend.clone();
    let thread = std::thread::Builder::new()
        .name("ferrum-jni-server".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runtime.block_on(async {
                    let listener =
                        tokio::net::TcpListener::from_std(listener).map_err(|e| e.to_string())?;
                    spark_connect_server::serve_with_backend(
                        listener,
                        worker_backend.server_backend(),
                        async {
                            let _ = stopped.await;
                        },
                    )
                    .await
                    .map_err(|e| e.to_string())
                })
            }))
            .unwrap_or_else(|_| Err("Rust listener panicked".into()));
            worker_backend.shutdown();
            // Never destroy a JVM global reference or runtime while attached work remains.
            worker_backend.wait_drained();
            runtime.shutdown_timeout(Duration::from_secs(1));
            let _ = done.send(result);
        })
        .map_err(|e| e.to_string())?;
    servers()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(
            id,
            Arc::new(ServerEntry {
                address,
                shutdown: Mutex::new(Some(shutdown)),
                done: Mutex::new(completed),
                thread: Mutex::new(Some(thread)),
                stopping: Mutex::new(()),
                drained: AtomicBool::new(false),
                backend,
            }),
        );
    Ok(id)
}
fn find_server(id: i64) -> Result<Arc<ServerEntry>, String> {
    servers()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&id)
        .cloned()
        .ok_or_else(|| "invalid or closed native server handle".into())
}
fn stop_server(id: i64) -> Result<(), String> {
    let server = find_server(id)?;
    let _stopping = server
        .stopping
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if server
        .thread
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_none()
    {
        if !server.drained.load(Ordering::SeqCst) {
            return Err("JNI server drain state is unknown after a listener-thread failure; references retained until JVM exit".into());
        }
        servers()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
        return Ok(());
    }
    server.backend.shutdown();
    if let Some(shutdown) = server
        .shutdown
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        let _ = shutdown.send(());
    }
    let result = match server.done.lock().unwrap_or_else(std::sync::PoisonError::into_inner).recv_timeout(Duration::from_secs(10)) {
        Ok(result) => {
            // Only the normal completion channel certifies that all activity
            // drained before the runtime stopped. A joined thread alone cannot.
            server.drained.store(true, Ordering::SeqCst);
            result
        },
        Err(std_mpsc::RecvTimeoutError::Timeout) => return Err("JNI stop is incomplete; active JVM calls retained safely, retry close after they finish".into()),
        Err(std_mpsc::RecvTimeoutError::Disconnected) => Err("JNI server completion channel closed without confirming teardown; drain state unknown".into()),
    };
    let mut result = result;
    if let Some(thread) = server
        .thread
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    {
        if thread.join().is_err() {
            result = Err("JNI server thread panicked".into());
        }
    }
    // NativeServer retains its handle on any error. Keep even a fully stopped
    // entry until the owner retries, rather than returning a stale handle.
    if result.is_ok() {
        servers()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }
    result
}

unsafe fn write_buffer(out: *mut BridgeBuffer, bytes: &[u8]) {
    if out.is_null() {
        return;
    }
    if bytes.is_empty() {
        *out = BridgeBuffer::default();
        return;
    }
    let data = libc::malloc(bytes.len()).cast::<u8>();
    if data.is_null() {
        *out = BridgeBuffer::default();
        return;
    }
    ptr::copy_nonoverlapping(bytes.as_ptr(), data, bytes.len());
    *out = BridgeBuffer {
        data,
        len: bytes.len(),
    };
}
fn ffi_result<T>(
    error: *mut BridgeBuffer,
    action: impl FnOnce() -> Result<T, String>,
) -> Option<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)) {
        Ok(Ok(result)) => Some(result),
        Ok(Err(message)) => {
            unsafe {
                write_buffer(error, message.as_bytes());
            }
            None
        }
        Err(_) => {
            unsafe {
                write_buffer(error, b"panic contained at JNI boundary");
            }
            None
        }
    }
}

/// # Safety
/// Called by bridge.c with a readable address and an owned bridge global reference.
#[no_mangle]
pub unsafe extern "C" fn ferrum_start(
    address: *const u8,
    length: usize,
    bridge: *mut c_void,
    error: *mut BridgeBuffer,
) -> i64 {
    ffi_result(error, || {
        if bridge.is_null() {
            return Err("null JVM bridge".into());
        }
        if address.is_null() && length != 0 {
            unsafe {
                bridge_free(bridge);
            }
            return Err("null listen address".into());
        }
        let address = if length == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(address, length)
        };
        start_server(address, bridge)
    })
    .unwrap_or(0)
}
/// # Safety
/// Output pointers must refer to writable BridgeBuffer objects owned by bridge.c.
#[no_mangle]
pub unsafe extern "C" fn ferrum_address(
    id: i64,
    out: *mut BridgeBuffer,
    error: *mut BridgeBuffer,
) -> i32 {
    ffi_result(error, || {
        let server = find_server(id)?;
        write_buffer(out, server.address.as_bytes());
        Ok(())
    })
    .map_or(-1, |_| 0)
}
/// # Safety
/// Error must be null or a writable BridgeBuffer owned by bridge.c.
#[no_mangle]
pub unsafe extern "C" fn ferrum_stop(id: i64, error: *mut BridgeBuffer) -> i32 {
    ffi_result(error, || stop_server(id)).map_or(-1, |_| 0)
}

/// # Safety
/// The JVM supplies valid JNI arguments for NativeServer.nativeStart.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_spark_sql_connect_nativebridge_NativeServer_nativeStart(
    env: *mut c_void,
    class: *mut c_void,
    address: *mut c_void,
    dispatcher: *mut c_void,
) -> i64 {
    bridge_native_start(env, class, address, dispatcher)
}
/// # Safety
/// The JVM supplies valid JNI arguments for NativeServer.nativeAddress.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_spark_sql_connect_nativebridge_NativeServer_nativeAddress(
    env: *mut c_void,
    class: *mut c_void,
    id: i64,
) -> *mut c_void {
    bridge_native_address(env, class, id)
}
/// # Safety
/// The JVM supplies valid JNI arguments for NativeServer.nativeStop.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_spark_sql_connect_nativebridge_NativeServer_nativeStop(
    env: *mut c_void,
    class: *mut c_void,
    id: i64,
) {
    bridge_native_stop(env, class, id)
}

/// # Safety
/// C supplies a readable address and transfers an owned typed Java engine handle.
#[no_mangle]
pub unsafe extern "C" fn ferrum_engine_start(
    address: *const u8,
    length: usize,
    factory: *mut c_void,
    error: *mut BridgeBuffer,
) -> i64 {
    ffi_result(error, || {
        if factory.is_null() {
            return Err("null Spark engine".into());
        }
        let backend = HostedBackend::Engine(engine::JvmEngine::new(factory));
        if address.is_null() && length != 0 {
            return Err("null listen address".into());
        }
        let address = if length == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(address, length)
        };
        start_host(address, backend)
    })
    .unwrap_or(0)
}

extern "C" {
    fn eng_native_start(
        env: *mut c_void,
        class: *mut c_void,
        address: *mut c_void,
        factory: *mut c_void,
    ) -> i64;
    fn eng_native_address(env: *mut c_void, class: *mut c_void, id: i64) -> *mut c_void;
    fn eng_native_stop(env: *mut c_void, class: *mut c_void, id: i64);
}
/// # Safety
/// The JVM supplies valid JNI arguments for NativeEngineServer.nativeStart.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_spark_sql_connect_nativebridge_engine_NativeEngineServer_nativeStart(
    env: *mut c_void,
    class: *mut c_void,
    address: *mut c_void,
    factory: *mut c_void,
) -> i64 {
    eng_native_start(env, class, address, factory)
}
/// # Safety
/// The JVM supplies valid JNI arguments for NativeEngineServer.nativeAddress.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_spark_sql_connect_nativebridge_engine_NativeEngineServer_nativeAddress(
    env: *mut c_void,
    class: *mut c_void,
    id: i64,
) -> *mut c_void {
    eng_native_address(env, class, id)
}
/// # Safety
/// The JVM supplies valid JNI arguments for NativeEngineServer.nativeStop.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_spark_sql_connect_nativebridge_engine_NativeEngineServer_nativeStop(
    env: *mut c_void,
    class: *mut c_void,
    id: i64,
) {
    eng_native_stop(env, class, id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_preserves_duplicates_and_raw_binary_values() {
        let mut metadata = MetadataMap::new();
        metadata.append("x-tag", "first".parse().unwrap());
        metadata.append("x-tag", "second".parse().unwrap());
        metadata.append_bin(
            "x-raw-bin",
            tonic::metadata::MetadataValue::from_bytes(b"\0\xff"),
        );
        let encoded = encode_metadata(&metadata).unwrap();
        let mut cursor = Cursor(&encoded);
        let decoded = cursor.metadata().unwrap();
        assert!(cursor.0.is_empty());
        assert_eq!(
            decoded
                .get_all("x-tag")
                .iter()
                .map(|v| v.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert_eq!(
            decoded
                .get_bin("x-raw-bin")
                .unwrap()
                .to_bytes()
                .unwrap()
                .as_ref(),
            b"\0\xff"
        );
    }

    #[test]
    fn close_event_preserves_code_unicode_and_structured_trailers() {
        let mut trailers = MetadataMap::new();
        trailers.append_bin(
            "grpc-status-details-bin",
            tonic::metadata::MetadataValue::from_bytes(b"\0\x12\xff"),
        );
        let mut encoded = vec![3];
        encoded.extend_from_slice(&(Code::InvalidArgument as u32).to_be_bytes());
        put_bytes(&mut encoded, "bad \0 query 😀".as_bytes()).unwrap();
        encoded.extend_from_slice(&encode_metadata(&trailers).unwrap());
        let Event::Close(status) = decode_event(encoded).unwrap() else {
            panic!("expected terminal status")
        };
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message(), "bad \0 query 😀");
        assert_eq!(status.details(), b"\0\x12\xff");
        let mut headers = tonic::codegen::http::HeaderMap::new();
        status.add_header(&mut headers).unwrap();
        assert_eq!(
            Status::from_header_map(&headers).unwrap().details(),
            b"\0\x12\xff"
        );
    }

    #[test]
    fn event_decoder_rejects_truncated_and_extra_fields() {
        for bytes in [
            vec![],
            vec![9],
            vec![1, 0, 0],
            vec![1, 0, 0, 0, 1],
            vec![1, 0, 0, 0, 0, 9],
            vec![3, 0, 0, 0, 3, 0, 0, 0, 1, 0xff, 0, 0, 0, 0],
        ] {
            assert!(decode_event(bytes).is_err());
        }
        assert!(
            matches!(decode_event(vec![2]).unwrap(), Event::Message(bytes) if bytes.is_empty())
        );
    }

    #[test]
    fn timeout_units_saturate_and_reject_invalid_headers() {
        let mut metadata = MetadataMap::new();
        assert_eq!(timeout_nanos(&metadata).unwrap(), -1);
        for (value, nanos) in [
            ("12n", 12),
            ("12u", 12_000),
            ("12m", 12_000_000),
            ("12S", 12_000_000_000),
            ("12M", 720_000_000_000),
            ("12H", 43_200_000_000_000),
            ("99999999H", i64::MAX),
        ] {
            metadata.insert("grpc-timeout", value.parse().unwrap());
            assert_eq!(timeout_nanos(&metadata).unwrap(), nanos);
        }
        for value in ["", "n", "-1S", "+1S", "12X", "123456789n"] {
            metadata.insert("grpc-timeout", value.parse().unwrap());
            assert!(timeout_nanos(&metadata).is_err(), "{value}");
        }
    }

    #[test]
    fn opening_calls_are_counted_before_jni_and_shutdown_rejects_new_calls() {
        // A null bridge is safe to release; this test never crosses into Java.
        let backend = DirectBackend::new(ptr::null_mut());
        let reservations = (0..MAX_CALLS)
            .map(|_| backend.reserve().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(*backend.state.active.lock().unwrap(), MAX_CALLS);
        assert!(
            matches!(backend.reserve(), Err(status) if status.code() == Code::ResourceExhausted)
        );
        let mut reservations = reservations;
        drop(reservations.pop());
        let replacement = backend.reserve().unwrap();
        backend.shutdown();
        assert!(matches!(backend.reserve(), Err(status) if status.code() == Code::Unavailable));
        let state = backend.state.clone();
        let (done, completed) = std_mpsc::channel();
        let waiter = std::thread::spawn(move || {
            state.wait_drained();
            done.send(()).unwrap();
        });
        assert!(completed.try_recv().is_err());
        drop(reservations);
        drop(replacement);
        completed.recv_timeout(Duration::from_secs(2)).unwrap();
        waiter.join().unwrap();
    }

    #[test]
    fn completed_server_errors_require_a_confirmed_drain_before_retry_succeeds() {
        for (id, disconnect) in [(-1, false), (-2, true)] {
            let backend = Arc::new(DirectBackend::new(ptr::null_mut()));
            let (done, completed) = std_mpsc::channel();
            let thread = std::thread::spawn(move || {
                if !disconnect {
                    done.send(Err("listener failed".into())).unwrap();
                }
            });
            servers().lock().unwrap().insert(
                id,
                Arc::new(ServerEntry {
                    address: "127.0.0.1:0".into(),
                    shutdown: Mutex::new(None),
                    done: Mutex::new(completed),
                    thread: Mutex::new(Some(thread)),
                    stopping: Mutex::new(()),
                    drained: AtomicBool::new(false),
                    backend: HostedBackend::Rpc(backend),
                }),
            );
            assert!(stop_server(id).is_err());
            assert!(find_server(id).unwrap().thread.lock().unwrap().is_none());
            if disconnect {
                let retry = stop_server(id).unwrap_err();
                assert!(retry.contains("drain state is unknown"), "{retry}");
                assert!(find_server(id).is_ok());
                // This synthetic entry owns no JNI objects; remove the test
                // fixture explicitly. Real unknown-drain entries stay retained.
                servers().lock().unwrap().remove(&id);
            } else {
                stop_server(id).unwrap();
                assert!(find_server(id).is_err());
            }
        }
    }

    #[tokio::test]
    async fn buffered_messages_precede_terminal_error_without_a_queue_slot() {
        let (sender, receiver) = mpsc::channel(2);
        sender.send(Ok(Bytes::from_static(b"one"))).await.unwrap();
        sender.send(Ok(Bytes::from_static(b"two"))).await.unwrap();
        let terminal = Arc::new(Mutex::new(Some(Status::resource_exhausted(
            "input too large",
        ))));
        drop(sender);
        let mut stream = Responses {
            receiver,
            terminal,
            _guard: CallGuard(Weak::new()),
        };
        assert_eq!(stream.next().await.unwrap().unwrap(), b"one"[..]);
        assert_eq!(stream.next().await.unwrap().unwrap(), b"two"[..]);
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            Code::ResourceExhausted
        );
        assert!(stream.next().await.is_none());
    }
}
