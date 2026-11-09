//! A Spark Connect transport proxy to an existing Spark JVM server.
//!
//! This backend forwards HTTP bodies without decoding Spark protobuf messages or
//! Arrow data. The JVM owns protocol sessions, operations, and query execution.

use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::future::poll_fn;
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::{BodyExt, StreamBody};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    body::Body,
    codegen::{http, Bytes, Service},
    server::NamedService,
    transport::{Channel, Endpoint},
    Status,
};

/// Forward Spark Connect RPCs to an existing JVM server.
///
/// Spark payloads, response metadata, and trailer frames pass through unchanged.
/// Tonic sets the upstream URI origin, appends its user agent, and applies the
/// incoming `grpc-timeout` while awaiting response headers. A timeout at the
/// proxy's tonic ingress can return `CANCELLED`; a channel header-wait timeout
/// maps to generic `UNAVAILABLE`. Neither is Spark's `DEADLINE_EXCEEDED`.
/// No application-level
/// retry, message-size limit, or session bookkeeping is added by this backend.
#[derive(Clone, Debug)]
pub struct SparkJvm {
    channel: Channel,
}

impl SparkJvm {
    /// Connect to an HTTP endpoint such as `http://127.0.0.1:15002`.
    ///
    /// Initial connection establishment has a ten-second timeout. Use
    /// [`Self::from_channel`] for an endpoint with custom transport settings.
    /// Receive windows are 64 KiB per stream and 16 MiB per connection, keeping
    /// room for independent results when several clients stop reading. The
    /// shared connection budget is finite and can still be exhausted.
    /// A fixed stream window can limit remote throughput to roughly one window
    /// per round trip. Use [`Self::from_channel`] with larger receive windows
    /// when needed for remote upstreams.
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.as_ref().to_owned())?
            .connect_timeout(Duration::from_secs(10))
            .initial_stream_window_size(64 * 1024)
            .initial_connection_window_size(16 * 1024 * 1024)
            .http2_adaptive_window(false)
            .connect()
            .await?;
        Ok(Self::from_channel(channel))
    }

    /// Use a preconfigured tonic channel, retaining its transport settings.
    /// Its receive-window settings determine isolation from unread streams.
    pub fn from_channel(channel: Channel) -> Self {
        Self { channel }
    }

    pub(crate) fn service(&self, shutdown: watch::Receiver<bool>) -> JvmService {
        JvmService {
            channel: self.channel.clone(),
            shutdown,
        }
    }
}

#[derive(Clone)]
pub(crate) struct JvmService {
    channel: Channel,
    shutdown: watch::Receiver<bool>,
}

impl NamedService for JvmService {
    const NAME: &'static str = "spark.connect.SparkConnectService";
}

impl Service<http::Request<Body>> for JvmService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Each call owns a channel clone and waits for that clone's readiness.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let mut channel = self.channel.clone();
        let shutdown = self.shutdown.clone();
        Box::pin(async move {
            if *shutdown.borrow() {
                return Ok(Status::unavailable("Spark JVM proxy is shutting down").into_http());
            }

            let request = request.map(|body| relay_body(body, shutdown.clone()));
            let response = async {
                poll_fn(|cx| channel.poll_ready(cx)).await?;
                channel.call(request).await
            };
            let result = tokio::select! {
                biased;
                _ = wait_for_shutdown(shutdown.clone()) => {
                    return Ok(Status::cancelled("Spark JVM proxy is shutting down").into_http());
                }
                result = response => result,
            };

            Ok(match result {
                Ok(response) => response.map(|body| relay_body(body, shutdown)),
                // Transport errors can contain endpoint addresses or credentials.
                // Upstream gRPC statuses arrive in headers/trailers and bypass this.
                Err(_) => Status::unavailable("Spark JVM backend is unavailable").into_http(),
            })
        })
    }
}

type Shutdown = Pin<Box<dyn Future<Output = ()> + Send>>;

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn relay_body(mut body: Body, shutdown: watch::Receiver<bool>) -> Body {
    if body.is_end_stream() {
        return body;
    }

    // Bound queued HTTP frames, without collecting or parsing gRPC messages.
    let (sender, receiver) = mpsc::channel(2);
    let pump_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let stopped = wait_for_shutdown(pump_shutdown);
        tokio::pin!(stopped);
        loop {
            let frame = tokio::select! {
                biased;
                _ = &mut stopped => return,
                _ = sender.closed() => return,
                frame = body.frame() => frame,
            };
            let Some(frame) = frame else {
                return;
            };
            let failed = frame.is_err();
            tokio::select! {
                biased;
                _ = &mut stopped => return,
                _ = sender.closed() => return,
                result = sender.send(frame) => {
                    if result.is_err() {
                        return;
                    }
                }
            }
            if failed {
                return;
            }
        }
    });

    // The pump drops the upstream body on shutdown even when a stalled peer
    // prevents this receiver body from being polled. Dropping the receiver also
    // wakes sender.closed(), so each pump follows its HTTP body's lifetime.
    Body::new(ShutdownBody {
        body: Some(Body::new(StreamBody::new(ReceiverStream::new(receiver)))),
        shutdown: Box::pin(wait_for_shutdown(shutdown)),
    })
}

struct ShutdownBody {
    body: Option<Body>,
    shutdown: Shutdown,
}

impl HttpBody for ShutdownBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.body.is_none() {
            return Poll::Ready(None);
        }
        if self.shutdown.as_mut().poll(cx).is_ready() {
            self.body.take();
            return Poll::Ready(Some(Err(Status::cancelled(
                "Spark JVM proxy is shutting down",
            ))));
        }
        Pin::new(self.body.as_mut().expect("body checked above")).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.as_ref().is_none_or(HttpBody::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        self.body
            .as_ref()
            .map_or_else(|| SizeHint::with_exact(0), HttpBody::size_hint)
    }
}
