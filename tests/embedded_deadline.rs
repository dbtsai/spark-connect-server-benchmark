//! Observe the embedded listener's ingress timeout without a client-side timer.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use spark_connect_server::{
    embedded::{RpcBackend, RpcStream},
    Backend,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::JoinHandle,
};
use tonic::{codegen::http, Code, Request, Response, Status};

struct Pending {
    calls: AtomicUsize,
    dropped: Arc<AtomicUsize>,
    stopped: watch::Sender<bool>,
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tonic::async_trait]
impl RpcBackend for Pending {
    async fn call(
        &self,
        method: String,
        request: Request<RpcStream>,
    ) -> Result<Response<RpcStream>, Status> {
        assert_eq!(method, "spark.connect.SparkConnectService/AnalyzePlan");
        assert_eq!(request.metadata().get("grpc-timeout").unwrap(), "100m");
        let mut stopped = self.stopped.subscribe();
        if *stopped.borrow() {
            return Err(Status::unavailable("test backend is closed"));
        }
        let _dropped = Dropped(self.dropped.clone());
        self.calls.fetch_add(1, Ordering::SeqCst);
        // This backend has no deadline timer and never supplies response headers.
        let _ = stopped.wait_for(|closed| *closed).await;
        Err(Status::cancelled("test backend is closed"))
    }

    fn shutdown(&self) {
        self.stopped.send_replace(true);
    }
}

struct Task(JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_deadline_before_headers_reports_ingress_cancelled() {
    let backend = Arc::new(Pending {
        calls: AtomicUsize::new(0),
        dropped: Arc::default(),
        stopped: watch::channel(false).0,
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stopped) = oneshot::channel();
    let server_backend = backend.clone();
    let mut server = Task(tokio::spawn(async move {
        spark_connect_server::serve_with_backend(
            listener,
            Backend::Embedded(server_backend),
            async {
                let _ = stopped.await;
            },
        )
        .await
        .unwrap();
    }));
    // A tonic Channel enforces grpc-timeout locally too. Raw h2 exposes the
    // listener's actual wire status, bounded only by this test's three seconds.
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
    let started = Instant::now();
    let (response, _) = client.send_request(request, true).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(3), response)
        .await
        .expect("embedded listener must honor the deadline")
        .unwrap();
    let elapsed = started.elapsed();
    let status = Status::from_header_map(response.headers()).expect("trailers-only timeout");
    // tonic 0.14 maps its ingress TimeoutExpired error to CANCELLED, independently
    // of the embedded backend's own deadline status.
    assert_eq!(status.code(), Code::Cancelled);
    assert_eq!(status.message(), "Timeout expired");
    assert!(elapsed >= Duration::from_millis(50), "elapsed: {elapsed:?}");
    assert!(elapsed < Duration::from_secs(3), "elapsed: {elapsed:?}");
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.dropped.load(Ordering::SeqCst), 1);
    eprintln!(
        "embedded ingress deadline: {:?}, {:?}, elapsed {elapsed:?}",
        status.code(),
        status.message()
    );
    drop(response);
    drop(client);
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), &mut server.0)
        .await
        .unwrap()
        .unwrap();
}
