//! Real gRPC checks of the in-process protobuf byte-stream boundary.

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::{future::poll_fn, Stream, StreamExt};
use http_body::Frame;
use http_body_util::{BodyExt, Full, StreamBody};
use spark_connect_server::{
    embedded::{RpcBackend, RpcStream},
    serve_with_backend, Backend,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::timeout,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    body::Body,
    codegen::{http, Bytes, Service},
    metadata::{MetadataMap, MetadataValue},
    transport::Channel,
    Code, Request, Response, Status,
};

const WAIT: Duration = Duration::from_secs(5);

struct FakeBackend {
    calls: Mutex<Vec<(String, MetadataMap)>>,
    stopped: watch::Sender<bool>,
    shutdowns: AtomicUsize,
    input_finished: Arc<AtomicUsize>,
    live_dropped: Arc<AtomicUsize>,
    pending_started: AtomicUsize,
    pending_dropped: Arc<AtomicUsize>,
}

impl Default for FakeBackend {
    fn default() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            stopped: watch::channel(false).0,
            shutdowns: AtomicUsize::new(0),
            input_finished: Arc::default(),
            live_dropped: Arc::default(),
            pending_started: AtomicUsize::new(0),
            pending_dropped: Arc::default(),
        }
    }
}

struct Dropped(Arc<AtomicUsize>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct TrackedStream {
    inner: RpcStream,
    _dropped: Dropped,
}

impl Stream for TrackedStream {
    type Item = Result<Bytes, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

async fn stopped(mut signal: watch::Receiver<bool>) {
    let _ = signal.wait_for(|value| *value).await;
}

fn trailing_metadata() -> MetadataMap {
    let mut metadata = MetadataMap::new();
    metadata.insert("x-trailer", "complete".parse().unwrap());
    metadata.insert_bin("x-trailer-bin", MetadataValue::from_bytes(&[0, 0xff, 0x80]));
    metadata
}

#[tonic::async_trait]
impl RpcBackend for FakeBackend {
    async fn call(
        &self,
        method: String,
        request: Request<RpcStream>,
    ) -> Result<Response<RpcStream>, Status> {
        if *self.stopped.borrow() {
            return Err(Status::unavailable("fake backend is shutting down"));
        }
        self.calls
            .lock()
            .unwrap()
            .push((method.clone(), request.metadata().clone()));
        let stream: RpcStream = match method.rsplit('/').next().unwrap() {
            "Echo" => {
                let finished = self.input_finished.clone();
                Box::pin(
                    request
                        .into_inner()
                        .chain(futures::stream::once(async move {
                            finished.fetch_add(1, Ordering::SeqCst);
                            Err(Status::with_metadata(Code::Ok, "", trailing_metadata()))
                        })),
                )
            }
            "Fail" => {
                return Err(Status::with_details_and_metadata(
                    Code::InvalidArgument,
                    "invalid plan 🦀",
                    Bytes::from_static(b"structured-status-details"),
                    trailing_metadata(),
                ));
            }
            "Live" => Box::pin(TrackedStream {
                inner: Box::pin(
                    futures::stream::once(async { Ok(Bytes::from_static(b"live")) })
                        .chain(futures::stream::pending())
                        .take_until(stopped(self.stopped.subscribe())),
                ),
                _dropped: Dropped(self.live_dropped.clone()),
            }),
            "Pending" => {
                let _dropped = Dropped(self.pending_dropped.clone());
                self.pending_started.fetch_add(1, Ordering::SeqCst);
                stopped(self.stopped.subscribe()).await;
                return Err(Status::cancelled("fake backend stopped"));
            }
            _ => return Err(Status::unimplemented("unknown fake method")),
        };
        let mut response = Response::new(stream);
        response
            .metadata_mut()
            .insert("x-initial", "present".parse().unwrap());
        response
            .metadata_mut()
            .insert_bin("x-initial-bin", MetadataValue::from_bytes(&[0, 0xfe]));
        response
            .metadata_mut()
            .append("x-repeat", "one".parse().unwrap());
        response
            .metadata_mut()
            .append("x-repeat", "two".parse().unwrap());
        Ok(response)
    }

    fn shutdown(&self) {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        self.stopped.send_replace(true);
    }
}

struct TestServer {
    channel: Channel,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl TestServer {
    async fn start(backend: Arc<FakeBackend>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(serve_with_backend(
            listener,
            Backend::Embedded(backend),
            async {
                let _ = stopped.await;
            },
        ));
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            channel,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    async fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(mut task) = self.task.take() {
            let result = timeout(WAIT, &mut task).await;
            if result.is_err() {
                task.abort();
            }
            result
                .expect("server shutdown must finish")
                .unwrap()
                .unwrap();
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn request(method: &str, body: Body) -> http::Request<Body> {
    http::Request::builder()
        .method("POST")
        .version(http::Version::HTTP_2)
        .uri(format!("/spark.connect.SparkConnectService/{method}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(body)
        .unwrap()
}

async fn raw_call(mut channel: Channel, request: http::Request<Body>) -> http::Response<Body> {
    poll_fn(|cx| channel.poll_ready(cx)).await.unwrap();
    channel.call(request).await.unwrap()
}

async fn call(channel: Channel, request: http::Request<Body>) -> http::Response<Body> {
    timeout(WAIT, raw_call(channel, request))
        .await
        .expect("response headers must arrive")
}

fn wire(message: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0];
    bytes.extend_from_slice(&(message.len() as u32).to_be_bytes());
    bytes.extend_from_slice(message);
    bytes
}

async fn collect(body: Body) -> (Bytes, http::HeaderMap) {
    let collected = timeout(WAIT, body.collect())
        .await
        .expect("finite body must finish")
        .unwrap();
    let trailers = collected.trailers().cloned().unwrap_or_default();
    (collected.to_bytes(), trailers)
}

async fn wait_for(counter: &AtomicUsize, count: usize) {
    timeout(WAIT, async {
        while counter.load(Ordering::SeqCst) < count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("backend lifecycle event must occur");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opaque_and_empty_messages_initial_metadata_and_status_trailers_survive() {
    let backend = Arc::new(FakeBackend::default());
    let mut server = TestServer::start(backend.clone()).await;
    // Invalid session text and an unknown field must survive without a Spark
    // protobuf parse/re-encode in the Rust frontend. Empty messages count too.
    let messages: &[&[u8]] = &[b"\x0a\x03bad\xa2\x1f\x03\0\xff\x7f", b"", b"\xff\x80\0"];
    let bytes: Vec<u8> = messages.iter().flat_map(|message| wire(message)).collect();
    let body = Body::new(StreamBody::new(futures::stream::iter(vec![
        Ok::<_, Status>(Frame::data(Bytes::copy_from_slice(&bytes[..2]))),
        Ok(Frame::data(Bytes::copy_from_slice(&bytes[2..]))),
    ])));
    let mut incoming = request("Echo", body);
    incoming
        .headers_mut()
        .insert("authorization", "Bearer synthetic-token".parse().unwrap());
    incoming
        .headers_mut()
        .insert("x-request-bin", "AAH+/w==".parse().unwrap());
    incoming
        .headers_mut()
        .append("x-repeat", "first".parse().unwrap());
    incoming
        .headers_mut()
        .append("x-repeat", "second".parse().unwrap());
    let response = call(server.channel.clone(), incoming).await;
    let initial = MetadataMap::from_headers(response.headers().clone());
    assert_eq!(initial.get("x-initial").unwrap(), "present");
    assert_eq!(
        initial
            .get_bin("x-initial-bin")
            .unwrap()
            .to_bytes()
            .unwrap(),
        &[0, 0xfe][..]
    );
    assert_eq!(
        initial
            .get_all("x-repeat")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
    let (received, trailers) = collect(response.into_body()).await;
    assert_eq!(received, bytes);
    let terminal = Status::from_header_map(&trailers).unwrap();
    assert_eq!(terminal.code(), Code::Ok);
    assert_eq!(terminal.metadata().get("x-trailer").unwrap(), "complete");
    assert_eq!(
        terminal
            .metadata()
            .get_bin("x-trailer-bin")
            .unwrap()
            .to_bytes()
            .unwrap(),
        &[0, 0xff, 0x80][..]
    );
    {
        let calls = backend.calls.lock().unwrap();
        assert_eq!(calls[0].0, "spark.connect.SparkConnectService/Echo");
        assert_eq!(
            calls[0].1.get("authorization").unwrap(),
            "Bearer synthetic-token"
        );
        assert_eq!(
            calls[0]
                .1
                .get_bin("x-request-bin")
                .unwrap()
                .to_bytes()
                .unwrap(),
            &[0, 1, 0xfe, 0xff][..]
        );
        assert_eq!(
            calls[0]
                .1
                .get_all("x-repeat")
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }
    let failed = call(server.channel.clone(), request("Fail", Body::empty())).await;
    let status = Status::from_header_map(failed.headers()).unwrap();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "invalid plan 🦀");
    assert_eq!(status.details(), b"structured-status-details");
    assert_eq!(status.metadata().get("x-trailer").unwrap(), "complete");
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bidirectional_messages_arrive_before_upload_eof_and_disconnect_cancels_calls() {
    let backend = Arc::new(FakeBackend::default());
    let mut server = TestServer::start(backend.clone()).await;
    let (upload, chunks) = mpsc::channel::<Result<Frame<Bytes>, Status>>(1);
    let first = wire(b"first");
    upload
        .send(Ok(Frame::data(Bytes::from(first.clone()))))
        .await
        .unwrap();
    let mut response = call(
        server.channel.clone(),
        request(
            "Echo",
            Body::new(StreamBody::new(ReceiverStream::new(chunks))),
        ),
    )
    .await;
    let received = timeout(WAIT, async {
        let mut bytes = Vec::new();
        while bytes.len() < first.len() {
            bytes.extend_from_slice(
                &response
                    .body_mut()
                    .frame()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_data()
                    .unwrap(),
            );
        }
        bytes
    })
    .await
    .expect("response must stream before upload EOF");
    assert_eq!(received, first);
    assert_eq!(backend.input_finished.load(Ordering::SeqCst), 0);
    assert!(!upload.is_closed());
    let last = wire(b"last");
    upload
        .send(Ok(Frame::data(Bytes::from(last.clone()))))
        .await
        .unwrap();
    drop(upload);
    assert_eq!(collect(response.into_body()).await.0, last);

    let mut live = call(server.channel.clone(), request("Live", Body::empty())).await;
    timeout(WAIT, live.body_mut().frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(live);
    wait_for(&backend.live_dropped, 1).await;
    let pending = tokio::spawn(raw_call(
        server.channel.clone(),
        request("Pending", Body::empty()),
    ));
    wait_for(&backend.pending_started, 1).await;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    wait_for(&backend.pending_dropped, 1).await;
    let next = call(
        server.channel.clone(),
        request(
            "Echo",
            Body::new(Full::new(Bytes::from(wire(b"still usable")))),
        ),
    )
    .await;
    assert_eq!(collect(next.into_body()).await.0, wire(b"still usable"));
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_reaches_backend_and_it_rejects_later_work() {
    let backend = Arc::new(FakeBackend::default());
    let mut server = TestServer::start(backend.clone()).await;
    let pending = tokio::spawn(raw_call(
        server.channel.clone(),
        request("Pending", Body::empty()),
    ));
    wait_for(&backend.pending_started, 1).await;
    server.stop().await;
    wait_for(&backend.pending_dropped, 1).await;
    assert_eq!(backend.shutdowns.load(Ordering::SeqCst), 1);
    drop(timeout(WAIT, pending).await.unwrap().unwrap());
    let error = backend
        .call(
            "spark.connect.SparkConnectService/Echo".into(),
            Request::new(Box::pin(futures::stream::empty()) as RpcStream),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::Unavailable);
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_declared_message_is_rejected_before_reading_its_payload() {
    let backend = Arc::new(FakeBackend::default());
    let mut server = TestServer::start(backend).await;
    let mut header = vec![0];
    header.extend_from_slice(&(64 * 1024 * 1024_u32 + 1).to_be_bytes());
    let response = call(
        server.channel.clone(),
        request("Echo", Body::new(Full::new(Bytes::from(header)))),
    )
    .await;
    let (bytes, trailers) = collect(response.into_body()).await;
    assert!(bytes.is_empty());
    let status = Status::from_header_map(&trailers).unwrap();
    assert_eq!(status.code(), Code::OutOfRange);
    assert!(status.message().contains("67108864"));
    server.stop().await;
}
