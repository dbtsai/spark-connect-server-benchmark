pub mod embedded;
pub mod engine;
pub mod jvm;
mod schema;
mod service;

/// One backend for all sessions served by a listener. Session state stays in it.
#[non_exhaustive]
pub enum Backend {
    Native(std::sync::Arc<dyn engine::Engine>),
    SparkJvm(jvm::SparkJvm),
    Embedded(std::sync::Arc<dyn embedded::RpcBackend>),
}

impl std::fmt::Debug for Backend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Native(_) => formatter.debug_tuple("Native").finish_non_exhaustive(),
            Self::SparkJvm(jvm) => formatter.debug_tuple("SparkJvm").field(jvm).finish(),
            Self::Embedded(_) => formatter.debug_tuple("Embedded").finish_non_exhaustive(),
        }
    }
}

// tonic's generated async-trait methods duplicate Future's must_use annotation.
#[allow(clippy::double_must_use)]
pub mod grpc {
    include!(concat!(
        env!("OUT_DIR"),
        "/spark.connect.SparkConnectService.rs"
    ));
}

/// Serve Spark Connect requests until shutdown, with a fresh session registry.
pub async fn serve(
    listener: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), tonic::transport::Error> {
    serve_with_engine(
        listener,
        std::sync::Arc::new(engine::DataFusionEngine),
        shutdown,
    )
    .await
}

/// Serve with an injected native engine and a fresh session registry.
/// Each protocol session remains bound to the backend session created by this engine.
pub async fn serve_with_engine(
    listener: tokio::net::TcpListener,
    engine: std::sync::Arc<dyn engine::Engine>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), tonic::transport::Error> {
    serve_with_backend(listener, Backend::Native(engine), shutdown).await
}

/// Serve with a native engine, a Spark JVM connection, or an in-process RPC backend.
/// Embedded RPC backends receive protobuf byte streams and own call lifetimes.
pub async fn serve_with_backend(
    listener: tokio::net::TcpListener,
    backend: Backend,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), tonic::transport::Error> {
    let (cancelled, cancellation) = tokio::sync::watch::channel(false);
    let (router, native_service, embedded_backend) = match backend {
        Backend::Native(engine) => {
            let service = std::sync::Arc::new(service::ConnectService::new(engine));
            let router = tonic::transport::Server::builder().add_service(
                grpc::spark_connect_service_server::SparkConnectServiceServer::from_arc(
                    service.clone(),
                )
                .max_decoding_message_size(64 * 1024 * 1024),
            );
            (router, Some(service), None)
        }
        Backend::SparkJvm(jvm) => (
            tonic::transport::Server::builder().add_service(jvm.service(cancellation)),
            None,
            None,
        ),
        Backend::Embedded(backend) => (
            tonic::transport::Server::builder()
                .add_service(embedded::EmbeddedService::new(backend.clone())),
            None,
            Some(backend),
        ),
    };
    let (draining, drain_started) = tokio::sync::oneshot::channel();
    let server = router.serve_with_incoming_shutdown(
        tokio_stream::wrappers::TcpListenerStream::new(listener),
        async move {
            shutdown.await;
            cancelled.send_replace(true);
            if let Some(service) = native_service {
                service.cancel_all();
            }
            if let Some(backend) = embedded_backend {
                backend.shutdown();
            }
            let _ = draining.send(());
        },
    );
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result,
        _ = drain_started => {
            // A peer that stops reading must not prevent local process shutdown.
            tokio::time::timeout(std::time::Duration::from_secs(2), &mut server)
                .await.unwrap_or(Ok(()))
        }
    }
}
