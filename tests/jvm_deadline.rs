//! Observe tonic's ingress deadline mapping with a client that does not enforce it.

use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use spark_connect_server::{jvm::SparkJvm, Backend};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    body::Body,
    codegen::{http, Service},
    server::NamedService,
    transport::Server,
    Code, Status,
};

#[derive(Clone)]
struct Pending;

impl NamedService for Pending {
    const NAME: &'static str = "spark.connect.SparkConnectService";
}

impl Service<http::Request<Body>> for Pending {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Request<Body>) -> Self::Future {
        Box::pin(std::future::pending())
    }
}

struct Task(JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_deadline_reports_a_documented_local_timeout_status() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", upstream.local_addr().unwrap());
    let _upstream_task = Task(tokio::spawn(async move {
        Server::builder()
            .add_service(Pending)
            .serve_with_incoming(TcpListenerStream::new(upstream))
            .await
            .unwrap();
    }));
    let backend = SparkJvm::connect(endpoint).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stopped) = oneshot::channel();
    let mut proxy = Task(tokio::spawn(async move {
        spark_connect_server::serve_with_backend(listener, Backend::SparkJvm(backend), async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    }));
    // A tonic Channel would also enforce grpc-timeout itself, obscuring the
    // response produced by the proxy. This raw h2 client only has the test bound.
    let (mut client, connection) =
        h2::client::handshake(TcpStream::connect(address).await.unwrap())
            .await
            .unwrap();
    let _connection = Task(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let request = http::Request::builder()
        .method("POST")
        .uri(format!(
            "http://{address}/spark.connect.SparkConnectService/AnalyzePlan"
        ))
        .header("content-type", "application/grpc")
        .header("grpc-timeout", "100m")
        .body(())
        .unwrap();
    let started = std::time::Instant::now();
    let (response, _) = client.send_request(request, true).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(3), response)
        .await
        .expect("proxy must honor the deadline")
        .unwrap();
    let status = Status::from_header_map(response.headers()).expect("trailers-only error response");
    // The ingress and upstream-channel timers enforce the same header. Their
    // ordering can select either documented local response on a timer tick.
    match status.code() {
        Code::Cancelled => assert_eq!(status.message(), "Timeout expired"),
        Code::Unavailable => assert_eq!(status.message(), "Spark JVM backend is unavailable"),
        code => panic!("unexpected local timeout status: {code:?}"),
    }
    assert!(started.elapsed() >= Duration::from_millis(50));
    drop(response);
    drop(client);
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), &mut proxy.0)
        .await
        .unwrap()
        .unwrap();
}
