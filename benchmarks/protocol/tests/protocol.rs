//! Exercise the official client and both streaming directions over loopback gRPC.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use spark_connect_protocol_bench::corpus;
use spark_connect_protocol_bench::fixture::Fixture;
use spark_connect_protocol_bench::grpc::spark_connect_service_server::SparkConnectServiceServer;
use spark_connect_protocol_bench::probes;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

struct TemporaryDirectory(PathBuf);

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct RunningServer(JoinHandle<Result<(), tonic::transport::Error>>);

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn official_client_probes_cover_all_routes_and_release_retained_state() {
    let directory = TemporaryDirectory(
        std::env::temp_dir().join(format!("protocol-fixture-probes-{}", uuid::Uuid::new_v4())),
    );
    let cases = corpus::build("smoke").unwrap();
    corpus::export(&cases, &directory.0).unwrap();
    let fixture = Arc::new(Fixture::load(&directory.0.join("manifest.json")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("protocol integration tests require a loopback listener");
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut server = RunningServer(tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(SparkConnectServiceServer::from_arc(fixture.clone()))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            }),
    ));

    let report = probes::run(
        &format!("http://{address}"),
        "fixture",
        &directory.0,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(report["passed"], true, "{report:#}");
    assert_eq!(report["routes_tested"], 12);
    assert_eq!(report["query_execution_requested"], false);
    let records = report["records"].as_array().unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record["route"] == "AnalyzePlan")
            .count(),
        14
    );
    for (route, variant) in [
        ("ReattachExecute", "resume_after_released_prefix"),
        ("AddArtifacts", "chunked_crc"),
        ("ArtifactStatus", "presence_and_failed_crc_discard"),
        ("CloneSession", "configuration_copy_isolation"),
        ("FetchErrorDetails", "fixture-error"),
        ("Interrupt", "operation_id"),
        ("Interrupt", "tag"),
        ("Interrupt", "all"),
        ("ReleaseSession", "tombstone"),
    ] {
        assert!(records
            .iter()
            .any(|record| record["route"] == route && record["variant"] == variant));
    }

    // Check before server shutdown so cleanup cannot hide leaked state.
    let stats = fixture.stats();
    for field in [
        "query_executions",
        "sessions",
        "retained_operations",
        "live_operations",
        "artifacts",
    ] {
        assert_eq!(stats[field], 0, "{field}: {stats:#}");
    }
    assert!(stats["calls"]
        .as_object()
        .unwrap()
        .values()
        .all(|calls| calls.as_u64().unwrap() > 0));
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(5), &mut server.0)
        .await
        .expect("fixture listener did not stop")
        .unwrap()
        .unwrap();
}
