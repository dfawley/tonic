use std::{
    collections::HashMap,
    error::Error,
    str::FromStr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{
    client::{
        name_resolution::{
            Address, ChannelController, Resolver, ResolverBuilder, ResolverData, ResolverOptions,
            ResolverUpdate, SharedResolverBuilder, GLOBAL_RESOLVER_REGISTRY, TCP_IP_ADDRESS_TYPE,
        },
        transport::{self, ConnectedTransport, GLOBAL_TRANSPORT_REGISTRY},
    },
    server,
    service::{Request, Response, Service},
};
use http::Uri;
use once_cell::sync::Lazy;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex, Notify},
    time::sleep,
};
use tonic::transport::Endpoint as TonicEndpoint;
use tonic::{async_trait, client::Grpc};

struct TonicTransportBuilder {}

impl TonicTransportBuilder {
    fn new() -> Self {
        Self {}
    }
}

struct Svc {
    conn: TcpStream,
}

struct ConnectedTonicTransport {
    grpc: Grpc<Svc>,
}

#[async_trait]
impl Service for ConnectedTonicTransport {
    async fn call(&self, request: Request) -> Response {
        todo!()
    }
}

#[async_trait]
impl ConnectedTransport for ConnectedTonicTransport {
    async fn disconnected(&self) {
        sleep(Duration::from_secs(10)).await; // TODO
    }
}

#[async_trait]
impl transport::Transport for TonicTransportBuilder {
    async fn connect(&self, address: String) -> Result<Box<dyn ConnectedTransport>, String> {
        let conn = TcpStream::connect(&address)
            .await
            .map_err(|e| e.to_string())?; // TODO: err msg
        let grpc = Grpc::with_origin(
            Svc { conn },
            Uri::from_maybe_shared(format!("http://{address}")).map_err(|e| e.to_string())?, // TODO: err msg
        );
        Ok(Box::new(ConnectedTonicTransport { grpc }))
    }
}

pub fn reg() {
    GLOBAL_TRANSPORT_REGISTRY.add_transport(TCP_IP_ADDRESS_TYPE, TonicTransportBuilder::new());
}
