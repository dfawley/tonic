use std::{
    collections::HashMap,
    error::Error,
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
use once_cell::sync::Lazy;
use tokio::{
    sync::{mpsc, oneshot, Mutex, Notify},
    time::sleep,
};
use tonic::async_trait;
use tonic::transport::Endpoint as TonicEndpoint;

struct TonicTransportBuilder {}

impl TonicTransportBuilder {
    fn new() -> Self {
        Self {}
    }
}

struct ConnectedTonicTransport {
    endpoint: TonicEndpoint,
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
        let endpoint = TonicEndpoint::from_shared("https://".to_string() + &address)
            .map_err(|e| e.to_string())?;
        endpoint.connect().await.map_err(|e| e.to_string())?;
        Ok(Box::new(ConnectedTonicTransport { endpoint }))
    }
}

pub fn reg() {
    GLOBAL_TRANSPORT_REGISTRY.add_transport(TCP_IP_ADDRESS_TYPE, TonicTransportBuilder::new());
}
