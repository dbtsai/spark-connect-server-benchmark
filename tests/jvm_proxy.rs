//! Raw HTTP/2 acceptance tests for the JVM transport, with no Spark decoder.

use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::future::poll_fn;
use http_body::{Body as HttpBody, Frame};
use http_body_util::{BodyExt, Full, StreamBody};
use prost::Message;
use spark_connect_server::{jvm::SparkJvm, serve_with_backend, Backend};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{
    body::Body,
    codegen::{http, Bytes, Service},
    server::NamedService,
    transport::{Channel, Endpoint, Server},
    Code, Status,
};

const WAIT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct State {
    recorded: Mutex<Vec<Recorded>>,
    headers_started: AtomicUsize,
    headers_dropped: Arc<AtomicUsize>,
    upload_started: AtomicUsize,
    upload_dropped: Arc<AtomicUsize>,
    duplex_finished: Arc<AtomicUsize>,
    duplex_dropped: Arc<AtomicUsize>,
    infinite_polled: Arc<AtomicUsize>,
    infinite_dropped: Arc<AtomicUsize>,
}

struct Recorded {
    path: String,
    headers: http::HeaderMap,
    bytes: Vec<u8>,
    trailers: http::HeaderMap,
}

struct Dropped(Arc<AtomicUsize>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct Upstream(Arc<State>);

impl NamedService for Upstream {
    const NAME: &'static str = "spark.connect.SparkConnectService";
}

impl Service<http::Request<Body>> for Upstream {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let state = self.0.clone();
        Box::pin(async move {
            let mode = request.headers()["x-mode"].to_str().unwrap().to_owned();
            let response = match mode.as_str() {
                "pending-headers" => {
                    let _dropped = Dropped(state.headers_dropped.clone());
                    state.headers_started.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<http::Response<Body>>().await
                }
                "pending-upload" => {
                    let _dropped = Dropped(state.upload_dropped.clone());
                    let mut body = request.into_body();
                    let first = body.frame().await.unwrap().unwrap();
                    assert!(first.is_data());
                    state.upload_started.fetch_add(1, Ordering::SeqCst);
                    while let Some(Ok(_)) = body.frame().await {}
                    response(Body::empty())
                }
                "duplex" => response(Body::new(Duplex {
                    input: request.into_body(),
                    finished: state.duplex_finished.clone(),
                    done: false,
                    _dropped: Dropped(state.duplex_dropped.clone()),
                })),
                "infinite" => response(Body::new(Infinite {
                    data: Bytes::from(vec![0x5a; 1024 * 1024]),
                    polled: state.infinite_polled.clone(),
                    _dropped: Dropped(state.infinite_dropped.clone()),
                })),
                "echo" => {
                    let (parts, body) = request.into_parts();
                    let (bytes, trailers) = collect(body).await;
                    state.recorded.lock().unwrap().push(Recorded {
                        path: parts.uri.path_and_query().unwrap().as_str().to_owned(),
                        headers: parts.headers,
                        bytes: bytes.clone(),
                        trailers,
                    });
                    response(frames_body(vec![
                        Frame::data(Bytes::from(bytes)),
                        Frame::trailers(error_trailers()),
                    ]))
                }
                _ => panic!("unexpected fake upstream mode: {mode}"),
            };
            Ok(response)
        })
    }
}

struct Duplex {
    input: Body,
    finished: Arc<AtomicUsize>,
    done: bool,
    _dropped: Dropped,
}

impl HttpBody for Duplex {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if self.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.input).poll_frame(cx) {
            Poll::Ready(None) => {
                self.done = true;
                self.finished.fetch_add(1, Ordering::SeqCst);
                let mut trailers = http::HeaderMap::new();
                trailers.insert("grpc-status", "0".parse().unwrap());
                Poll::Ready(Some(Ok(Frame::trailers(trailers))))
            }
            frame => frame,
        }
    }
}

struct Infinite {
    data: Bytes,
    polled: Arc<AtomicUsize>,
    _dropped: Dropped,
}

impl HttpBody for Infinite {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        self.polled.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Some(Ok(Frame::data(self.data.clone()))))
    }
}

struct TrackedBody {
    body: Body,
    _dropped: Dropped,
}

impl HttpBody for TrackedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }
}

struct RunningServer {
    address: std::net::SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl RunningServer {
    async fn upstream(state: Arc<State>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(
            Server::builder()
                .add_service(Upstream(state))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                }),
        );
        Self {
            address,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    async fn proxy(backend: SparkJvm) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(serve_with_backend(
            listener,
            Backend::SparkJvm(backend),
            async {
                let _ = stopped.await;
            },
        ));
        Self {
            address,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    async fn channel(&self) -> Channel {
        Endpoint::from_shared(self.endpoint())
            .unwrap()
            .initial_stream_window_size(1024)
            .http2_adaptive_window(false)
            .connect()
            .await
            .unwrap()
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
                .expect("server must stop within the deadline")
                .unwrap()
                .unwrap();
        }
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn response(body: Body) -> http::Response<Body> {
    http::Response::builder()
        .header("content-type", "application/grpc")
        .header("grpc-encoding", "opaque-test-codec")
        .header("x-upstream", "initial-metadata")
        .header("x-upstream-bin", "AAH+/w==")
        .header("x-repeat", "first")
        .header("x-repeat", "second")
        .body(body)
        .unwrap()
}

fn error_trailers() -> http::HeaderMap {
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", "7".parse().unwrap());
    trailers.insert("grpc-message", "opaque%20upstream%20error".parse().unwrap());
    trailers.insert(
        "grpc-status-details-bin",
        "CAcSBm9wYXF1ZQ==".parse().unwrap(),
    );
    trailers.insert("x-trailer", "retained".parse().unwrap());
    trailers.append("x-repeat", "trailer-one".parse().unwrap());
    trailers.append("x-repeat", "trailer-two".parse().unwrap());
    trailers
}

fn frames_body(frames: Vec<Frame<Bytes>>) -> Body {
    Body::new(StreamBody::new(futures::stream::iter(
        frames.into_iter().map(Ok::<_, Status>),
    )))
}

fn request(route: &str, mode: &str, body: Body) -> http::Request<Body> {
    http::Request::builder()
        .method("POST")
        .version(http::Version::HTTP_2)
        .uri(format!("/spark.connect.SparkConnectService/{route}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("x-mode", mode)
        .body(body)
        .unwrap()
}

async fn raw_call(
    mut channel: Channel,
    request: http::Request<Body>,
) -> Result<http::Response<Body>, tonic::transport::Error> {
    poll_fn(|cx| channel.poll_ready(cx)).await?;
    channel.call(request).await
}

async fn call(channel: Channel, request: http::Request<Body>) -> http::Response<Body> {
    timeout(WAIT, raw_call(channel, request))
        .await
        .expect("response headers must arrive within the deadline")
        .unwrap()
}

async fn collect(body: Body) -> (Vec<u8>, http::HeaderMap) {
    let collected = timeout(WAIT, body.collect())
        .await
        .expect("finite body must finish within the deadline")
        .unwrap();
    let trailers = collected.trailers().cloned().unwrap_or_default();
    (collected.to_bytes().to_vec(), trailers)
}

async fn wait_for(counter: &AtomicUsize, minimum: usize) {
    timeout(WAIT, async {
        while counter.load(Ordering::SeqCst) < minimum {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("upstream lifecycle event must occur within the deadline");
}

fn values(headers: &http::HeaderMap, name: &str) -> Vec<String> {
    headers
        .get_all(name)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect()
}

fn wire(payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0];
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_spark_routes_preserve_opaque_bytes_metadata_and_error_trailers() {
    let state = Arc::new(State::default());
    let mut upstream = RunningServer::upstream(state.clone()).await;
    let backend = SparkJvm::connect(upstream.endpoint()).await.unwrap();
    let mut proxy = RunningServer::proxy(backend).await;
    let channel = proxy.channel().await;
    let mut payload = spark_connect_proto::ExecutePlanRequest {
        session_id: "deliberately-invalid-session".into(),
        operation_id: Some("not-a-uuid-either".into()),
        ..Default::default()
    }
    .encode_to_vec();
    // Field 500 is unknown to the pinned protocol; its payload is opaque binary.
    payload.extend_from_slice(&[0xa2, 0x1f, 3, 0, 0xff, 0x7f]);
    let bytes = wire(&payload);
    let routes = [
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
        "FutureSparkRpc?extension=kept",
    ];
    for route in routes {
        let mut upload_trailers = http::HeaderMap::new();
        upload_trailers.insert("x-upload-trailer", "retained".parse().unwrap());
        let mut request = request(
            route,
            "echo",
            frames_body(vec![
                Frame::data(Bytes::copy_from_slice(&bytes[..3])),
                Frame::data(Bytes::copy_from_slice(&bytes[3..])),
                Frame::trailers(upload_trailers),
            ]),
        );
        for (name, value) in [
            ("authorization", "Bearer synthetic-test-token"),
            ("user-agent", "opaque-client/1"),
            ("x-ascii", "session metadata"),
            ("x-opaque-bin", "AAH+/w=="),
            ("grpc-encoding", "opaque-test-codec"),
            ("grpc-accept-encoding", "identity,opaque-test-codec"),
            ("grpc-timeout", "10S"),
        ] {
            request.headers_mut().insert(
                http::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        request
            .headers_mut()
            .append("x-repeat", "one".parse().unwrap());
        request
            .headers_mut()
            .append("x-repeat", "two".parse().unwrap());
        let response = call(channel.clone(), request).await;
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.headers()["grpc-encoding"], "opaque-test-codec");
        assert_eq!(response.headers()["x-upstream"], "initial-metadata");
        assert_eq!(response.headers()["x-upstream-bin"], "AAH+/w==");
        assert_eq!(values(response.headers(), "x-repeat"), ["first", "second"]);
        let (received, trailers) = collect(response.into_body()).await;
        assert_eq!(received, bytes);
        assert_eq!(trailers, error_trailers());
    }
    {
        let recorded = state.recorded.lock().unwrap();
        assert_eq!(recorded.len(), routes.len());
        for (record, route) in recorded.iter().zip(routes) {
            assert_eq!(
                record.path,
                format!("/spark.connect.SparkConnectService/{route}")
            );
            assert_eq!(record.bytes, bytes);
            assert_eq!(
                record.headers["authorization"],
                "Bearer synthetic-test-token"
            );
            assert_eq!(record.headers["x-ascii"], "session metadata");
            assert_eq!(record.headers["x-opaque-bin"], "AAH+/w==");
            assert_eq!(record.headers["grpc-encoding"], "opaque-test-codec");
            assert_eq!(
                record.headers["grpc-accept-encoding"],
                "identity,opaque-test-codec"
            );
            assert_eq!(record.headers["grpc-timeout"], "10S");
            assert_eq!(values(&record.headers, "x-repeat"), ["one", "two"]);
            assert!(record.headers["user-agent"]
                .to_str()
                .unwrap()
                .starts_with("opaque-client/1"));
            assert_eq!(record.trailers["x-upload-trailer"], "retained");
        }
    }
    drop(channel);
    proxy.stop().await;
    upstream.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_and_results_stream_live_and_disconnect_only_cancels_its_rpc() {
    let state = Arc::new(State::default());
    let mut upstream = RunningServer::upstream(state.clone()).await;
    let mut proxy =
        RunningServer::proxy(SparkJvm::connect(upstream.endpoint()).await.unwrap()).await;
    let channel = proxy.channel().await;
    let (upload, chunks) = mpsc::channel::<Result<Frame<Bytes>, Status>>(1);
    let first = wire(b"first opaque message");
    upload
        .send(Ok(Frame::data(Bytes::from(first.clone()))))
        .await
        .unwrap();
    let mut response = call(
        channel.clone(),
        request(
            "AddArtifacts",
            "duplex",
            Body::new(StreamBody::new(ReceiverStream::new(chunks))),
        ),
    )
    .await;
    let streamed = timeout(WAIT, async {
        let mut received = Vec::new();
        while received.len() < first.len() {
            received.extend_from_slice(
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
        received
    })
    .await
    .expect("first result must arrive before upload EOF");
    assert_eq!(streamed, first);
    assert_eq!(state.duplex_finished.load(Ordering::SeqCst), 0);
    assert!(!upload.is_closed());
    let second = wire(b"second message");
    upload
        .send(Ok(Frame::data(Bytes::from(second.clone()))))
        .await
        .unwrap();
    drop(upload);
    let (rest, trailers) = collect(response.into_body()).await;
    assert_eq!(rest, second);
    assert_eq!(trailers["grpc-status"], "0");
    wait_for(&state.duplex_dropped, 1).await;

    let mut abandoned = call(
        channel.clone(),
        request("ExecutePlan", "infinite", Body::empty()),
    )
    .await;
    let frame = timeout(WAIT, abandoned.body_mut().frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(frame.is_data());
    drop(abandoned);
    wait_for(&state.infinite_dropped, 1).await;
    let next = call(
        channel.clone(),
        request(
            "GetStatus",
            "echo",
            Body::new(Full::new(Bytes::from_static(b"still usable"))),
        ),
    )
    .await;
    assert_eq!(collect(next.into_body()).await.0, b"still usable");
    drop(channel);
    proxy.stop().await;
    upstream.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_drops_pending_headers_upload_and_an_unread_infinite_result() {
    let state = Arc::new(State::default());
    let mut upstream = RunningServer::upstream(state.clone()).await;
    let mut proxy =
        RunningServer::proxy(SparkJvm::connect(upstream.endpoint()).await.unwrap()).await;
    let channel = proxy.channel().await;
    let headers = tokio::spawn(raw_call(
        channel.clone(),
        request("AnalyzePlan", "pending-headers", Body::empty()),
    ));
    wait_for(&state.headers_started, 1).await;
    let upload_dropped = Arc::new(AtomicUsize::new(0));
    let (upload, chunks) = mpsc::channel::<Result<Frame<Bytes>, Status>>(1);
    let upload_body = Body::new(TrackedBody {
        body: Body::new(StreamBody::new(ReceiverStream::new(chunks))),
        _dropped: Dropped(upload_dropped.clone()),
    });
    let uploading = tokio::spawn(raw_call(
        channel.clone(),
        request("AddArtifacts", "pending-upload", upload_body),
    ));
    upload
        .send(Ok(Frame::data(Bytes::from_static(b"incomplete upload"))))
        .await
        .unwrap();
    wait_for(&state.upload_started, 1).await;
    assert!(!upload.is_closed());
    let unread = call(
        channel.clone(),
        request("ExecutePlan", "infinite", Body::empty()),
    )
    .await;
    wait_for(&state.infinite_polled, 1).await;
    // Retain the completely unread response. Shutdown must release upstream
    // state without any further client polling.
    proxy.stop().await;
    wait_for(&state.headers_dropped, 1).await;
    wait_for(&state.upload_dropped, 1).await;
    wait_for(&state.infinite_dropped, 1).await;
    drop(
        timeout(WAIT, headers)
            .await
            .expect("pending headers must stop")
            .unwrap(),
    );
    drop(
        timeout(WAIT, uploading)
            .await
            .expect("pending upload must stop")
            .unwrap(),
    );
    wait_for(&upload_dropped, 1).await;
    assert_eq!(unread.status(), http::StatusCode::OK);
    drop(unread);
    drop(upload);
    drop(channel);
    upstream.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_transport_failure_returns_a_generic_unavailable_status() {
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let channel = Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(100))
        .connect_lazy();
    let mut proxy = RunningServer::proxy(SparkJvm::from_channel(channel)).await;
    let response = call(
        proxy.channel().await,
        request("Config", "echo", Body::empty()),
    )
    .await;
    let status = Status::from_header_map(response.headers()).unwrap();
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(status.message(), "Spark JVM backend is unavailable");
    assert!(!status.message().contains(&address.to_string()));
    proxy.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn six_unread_results_allow_an_independent_large_response() {
    const UNREAD_STREAMS: usize = 6;
    const ECHO_BYTES: usize = 4 * 1024 * 1024;
    let state = Arc::new(State::default());
    let mut upstream = RunningServer::upstream(state.clone()).await;
    let mut proxy =
        RunningServer::proxy(SparkJvm::connect(upstream.endpoint()).await.unwrap()).await;
    let echo = Bytes::from(vec![0x3c; ECHO_BYTES]);
    // Keep the actively reading client on normal transport defaults. Only the
    // six stalled clients below deliberately advertise tiny receive windows.
    let independent = Endpoint::from_shared(proxy.endpoint())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let control = timeout(WAIT, async {
        let response = raw_call(
            independent.clone(),
            request("GetStatus", "echo", Body::new(Full::new(echo.clone()))),
        )
        .await
        .unwrap();
        response.into_body().collect().await.unwrap()
    })
    .await
    .expect("uncontended large echo must complete before testing unread streams");
    assert_eq!(control.to_bytes(), echo);
    eprintln!("uncontended echo bytes={ECHO_BYTES}, completed=true");
    let mut unread = Vec::new();
    let mut clients = Vec::new();
    for _ in 0..UNREAD_STREAMS {
        // Each downstream client has its own HTTP/2 connection and a 1 KiB
        // receive window, while all requests currently share one upstream.
        let channel = proxy.channel().await;
        unread.push(
            call(
                channel.clone(),
                request("ExecutePlan", "infinite", Body::empty()),
            )
            .await,
        );
        clients.push(channel);
    }
    // Each upstream poll offers a 1 MiB chunk. Six unread streams reproduce
    // exhaustion of the original 5 MiB upstream connection budget.
    wait_for(&state.infinite_polled, UNREAD_STREAMS).await;
    assert_eq!(state.infinite_dropped.load(Ordering::SeqCst), 0);
    let result = timeout(WAIT, async {
        let response = raw_call(
            independent.clone(),
            request("GetStatus", "echo", Body::new(Full::new(echo.clone()))),
        )
        .await
        .map_err(|error| error.to_string())?;
        response
            .into_body()
            .collect()
            .await
            .map_err(|error| error.to_string())
    })
    .await;
    eprintln!(
        "unread streams={UNREAD_STREAMS}, upstream MiB chunks offered={}, independent echo bytes={ECHO_BYTES}, completed={}",
        state.infinite_polled.load(Ordering::SeqCst),
        result.is_ok(),
    );
    // Preserve all unread responses until the independent request has either
    // completed or hit its deadline. Cleanup precedes the liveness assertion.
    drop(unread);
    wait_for(&state.infinite_dropped, UNREAD_STREAMS).await;
    drop(clients);
    drop(independent);
    proxy.stop().await;
    upstream.stop().await;
    let collected = result
        .expect("unread results must not block an independent client's large response")
        .expect("independent RPC must succeed");
    assert_eq!(collected.to_bytes(), echo);
}
