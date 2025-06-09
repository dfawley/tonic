use http::Uri;
use http_body::Body;
use hyper::{client::conn::http2::Builder, rt::Executor};
use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tower::{
    limit::{ConcurrencyLimitLayer, RateLimitLayer},
    util::BoxService,
    Layer, ServiceBuilder,
};
use tower_service::Service as TowerService;

use crate::{
    client::{
        name_resolution::TCP_IP_NETWORK_TYPE,
        transport::{self, ConnectedTransport, GLOBAL_TRANSPORT_REGISTRY},
    },
    rt::{
        self,
        hyper_wrapper::{HyperCompatExec, HyperCompatTimer, HyperStream},
        Runtime,
    },
    server,
    service::{Request, Response, Service},
};
use once_cell::sync::Lazy;
use tokio::{
    io::AsyncWriteExt,
    sync::{mpsc, oneshot, Mutex, Notify},
    time::sleep,
};
use tonic::{async_trait, client::Grpc, IntoRequest, Status};
use tonic::{client::GrpcService, transport::Endpoint as TonicEndpoint};

use super::SubchannelConfig;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

struct TonicTransportBuilder {}

impl TonicTransportBuilder {
    fn new() -> Self {
        Self {}
    }
}

struct ConnectedTonicTransport {
    grpc: Grpc<TonicService>,
    task_handle: Box<dyn rt::TaskHandle>,
    closed: Arc<Notify>,
}

impl Drop for ConnectedTonicTransport {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

#[async_trait]
impl Service for ConnectedTonicTransport {
    async fn call(&mut self, method: String, request: Request) -> Result<Response, Status> {
        self.grpc.ready().await.map_err(|e| {
            tonic::Status::unknown(format!("Service was not ready: {}", e.to_string()))
        })?;
        let path = http::uri::PathAndQuery::from_maybe_shared(method)
            .map_err(|err| tonic::Status::internal(format!("Failed to parse path: {}", err)))?;
        Ok(self.grpc.streaming(request, path, todo!()).await)
    }
}

#[async_trait]
impl ConnectedTransport for ConnectedTonicTransport {
    async fn disconnected(&self) {
        self.closed.notified().await
    }
}

#[async_trait]
impl transport::Transport for TonicTransportBuilder {
    async fn connect(
        &self,
        config: Arc<SubchannelConfig>,
    ) -> Result<Box<dyn ConnectedTransport>, String> {
        let runtime = config.runtime.clone();
        let mut settings = Builder::<HyperCompatExec>::new(HyperCompatExec {
            inner: runtime.clone(),
        })
        .timer(HyperCompatTimer {
            inner: runtime.clone(),
        })
        .initial_stream_window_size(config.init_stream_window_size)
        .initial_connection_window_size(config.init_connection_window_size)
        .keep_alive_interval(config.http2_keep_alive_interval)
        .clone();

        if let Some(val) = config.http2_keep_alive_timeout {
            settings.keep_alive_timeout(val);
        }

        if let Some(val) = config.http2_keep_alive_while_idle {
            settings.keep_alive_while_idle(val);
        }

        if let Some(val) = config.http2_adaptive_window {
            settings.adaptive_window(val);
        }

        if let Some(val) = config.http2_max_header_list_size {
            settings.max_header_list_size(val);
        }

        let addr: SocketAddr =
            SocketAddr::from_str(&config.address).map_err(|err| err.to_string())?;
        let tcp_stream_fut = runtime.tcp_stream(
            addr,
            rt::TcpOptions {
                enable_nodelay: config.tcp_nodelay,
                keepalive: config.tcp_keepalive,
            },
        );
        let tcp_stream = if let Some(timeout) = config.connect_timeout {
            tokio::select! {
            _ = runtime.sleep(timeout) => {
                return Err("timed out waiting for TCP stream to connect".to_string())
            }
            tcp_stream = tcp_stream_fut => { tcp_stream? }
            }
        } else {
            tcp_stream_fut.await?
        };
        let tcp_stream = HyperStream::new(tcp_stream);

        let (sender, connection) = settings
            .handshake(tcp_stream)
            .await
            .map_err(|err| err.to_string())?;
        let closed_notifier = Arc::new(Notify::new());
        let notifier_copy = closed_notifier.clone();

        let task_handle = runtime.spawn(Box::pin(async move {
            if let Err(err) = connection.await {
                println!("connection error: {:?}", err);
            }
            notifier_copy.notify_one();
        }));
        let sender = SendRequest::from(sender);

        let service = ServiceBuilder::new()
            .option_layer(config.concurrency_limit.map(ConcurrencyLimitLayer::new))
            .option_layer(config.rate_limit.map(|(l, d)| RateLimitLayer::new(l, d)))
            .map_err(Into::into)
            .service(sender);

        let service = BoxService::new(service);
        let uri = Uri::from_maybe_shared(format!("http://{}", config.address))
            .map_err(|e| e.to_string())?; // TODO: err msg
        let grpc = Grpc::with_origin(TonicService { inner: service }, uri);

        Ok(Box::new(ConnectedTonicTransport {
            grpc,
            task_handle,
            closed: closed_notifier,
        }))
    }
}

struct SendRequest {
    inner: hyper::client::conn::http2::SendRequest<tonic::body::Body>,
}

impl From<hyper::client::conn::http2::SendRequest<tonic::body::Body>> for SendRequest {
    fn from(inner: hyper::client::conn::http2::SendRequest<tonic::body::Body>) -> Self {
        Self { inner }
    }
}

impl tower::Service<http::Request<tonic::body::Body>> for SendRequest {
    type Response = http::Response<tonic::body::Body>;
    type Error = crate::BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let fut = self.inner.send_request(req);

        Box::pin(async move {
            fut.await
                .map_err(Into::into)
                .map(|res| res.map(tonic::body::Body::new))
        })
    }
}

pub fn reg() {
    GLOBAL_TRANSPORT_REGISTRY.add_transport(TCP_IP_NETWORK_TYPE, TonicTransportBuilder::new());
}

struct TonicService {
    inner: BoxService<
        http::Request<tonic::body::Body>,
        http::Response<tonic::body::Body>,
        crate::BoxError,
    >,
}

impl GrpcService<tonic::body::Body> for TonicService {
    type ResponseBody = tonic::body::Body;
    type Error = crate::BoxError;
    type Future = BoxFuture<'static, Result<http::Response<Self::ResponseBody>, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        tower::Service::call(&mut self.inner, request)
    }
}
