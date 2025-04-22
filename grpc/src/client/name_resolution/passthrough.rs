use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use crate::{
    client::name_resolution::{
        Address, ChannelController, Endpoint, Resolver, ResolverBuilder, ResolverData,
        ResolverOptions, ResolverUpdate, SharedResolverBuilder, GLOBAL_RESOLVER_REGISTRY,
    },
    server,
    service::{Request, Response, Service},
};
use once_cell::sync::Lazy;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tonic::async_trait;

use super::TCP_IP_ADDRESS_TYPE;

pub fn reg() {
    GLOBAL_RESOLVER_REGISTRY.add_builder(SharedResolverBuilder::new(PassthroughResolverBuilder));
}

struct PassthroughResolverBuilder;

impl ResolverBuilder for PassthroughResolverBuilder {
    fn scheme(&self) -> &'static str {
        "passthrough"
    }

    fn build(
        &self,
        target: url::Url,
        resolve_now: Arc<Notify>,
        options: ResolverOptions,
    ) -> Box<dyn Resolver> {
        let id = target.path().strip_prefix("/").unwrap().to_string();

        Box::new(NopResolver { id })
    }
}

struct NopResolver {
    id: String,
}

#[async_trait]
impl Resolver for NopResolver {
    async fn run(&mut self, channel_controller: Box<dyn ChannelController>) {
        let _ = channel_controller
            .update(ResolverUpdate::Data(ResolverData {
                endpoints: vec![Endpoint {
                    addresses: vec![Address {
                        address_type: TCP_IP_ADDRESS_TYPE.to_string(),
                        address: self.id.clone(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await;
    }
}
