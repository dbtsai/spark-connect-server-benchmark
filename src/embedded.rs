//! An in-process RPC boundary for backends such as an embedded JVM.
//!
//! Tonic owns gRPC framing while the backend owns protobuf parsing, method
//! dispatch, and call lifetimes. Spark messages remain opaque Rust byte buffers.

use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use prost::bytes::{Buf, BufMut};
use tonic::{
    body::Body,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    codegen::{http, Bytes, Service},
    server::{Grpc, NamedService, StreamingService},
    Request, Response, Status, Streaming,
};

const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// A stream of unframed protobuf messages, with no Spark-specific decoding.
///
/// A terminal error supplies the gRPC status and trailing metadata. To supply
/// custom trailers on success, end with `Err(Status::with_metadata(Code::Ok,
/// "", trailers))`; ordinary stream completion produces the default OK status.
pub type RpcStream = Pin<Box<dyn Stream<Item = Result<Bytes, Status>> + Send>>;

/// Dispatch protobuf byte streams directly to an in-process RPC implementation.
// async-trait repeats Future's must_use annotation on generated methods.
#[allow(clippy::double_must_use)]
#[tonic::async_trait]
pub trait RpcBackend: Send + Sync {
    /// Handle a fully qualified method such as
    /// `spark.connect.SparkConnectService/ExecutePlan`.
    ///
    /// Requests retain their initial metadata and extensions. Backends own
    /// bounded buffering and cancellation when a pending call or returned
    /// response stream is dropped. Request trailers are not exposed by this
    /// byte-stream interface.
    async fn call(
        &self,
        method: String,
        request: Request<RpcStream>,
    ) -> Result<Response<RpcStream>, Status>;

    /// Reject new calls and signal cancellation of all existing calls, including
    /// streams a peer stops reading. This must return promptly so the listener
    /// can finish draining.
    fn shutdown(&self);
}

#[derive(Clone)]
pub(crate) struct EmbeddedService {
    backend: Arc<dyn RpcBackend>,
}

impl EmbeddedService {
    pub(crate) fn new(backend: Arc<dyn RpcBackend>) -> Self {
        Self { backend }
    }
}

impl NamedService for EmbeddedService {
    const NAME: &'static str = "spark.connect.SparkConnectService";
}

impl Service<http::Request<Body>> for EmbeddedService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let call = BackendCall {
            backend: self.backend.clone(),
            method: request.uri().path().trim_start_matches('/').to_owned(),
        };
        Box::pin(async move {
            let response = Grpc::new(BytesCodec)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES)
                .streaming(call, request)
                .await;
            Ok(response)
        })
    }
}

struct BackendCall {
    backend: Arc<dyn RpcBackend>,
    method: String,
}

impl StreamingService<Bytes> for BackendCall {
    type Response = Bytes;
    type ResponseStream = RpcStream;
    type Future = Pin<Box<dyn Future<Output = Result<Response<RpcStream>, Status>> + Send>>;

    fn call(&mut self, request: Request<Streaming<Bytes>>) -> Self::Future {
        let backend = self.backend.clone();
        let method = self.method.clone();
        Box::pin(async move {
            backend
                .call(method, request.map(|stream| Box::pin(stream) as RpcStream))
                .await
        })
    }
}

struct BytesCodec;

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesEncoder;
    type Decoder = BytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        BytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        BytesDecoder
    }
}

struct BytesEncoder;

impl Encoder for BytesEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, message: Bytes, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(&message);
        Ok(())
    }
}

struct BytesDecoder;

impl Decoder for BytesDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(source.copy_to_bytes(source.remaining())))
    }
}
