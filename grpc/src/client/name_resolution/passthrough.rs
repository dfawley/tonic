use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use crate::{
    client::name_resolution::{
        Address, ChannelController, Endpoint, Resolver, ResolverBuilder, ResolverOptions,
        ResolverUpdate, GLOBAL_RESOLVER_REGISTRY,
    },
    server,
    service::{Request, Response, Service},
};
use once_cell::sync::Lazy;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tonic::async_trait;

use super::TCP_IP_NETWORK_TYPE;

pub fn reg() {
    GLOBAL_RESOLVER_REGISTRY.add_builder(Box::new(PassthroughResolverBuilder));
}

struct PassthroughResolverBuilder;

impl ResolverBuilder for PassthroughResolverBuilder {
    fn scheme(&self) -> &'static str {
        "passthrough"
    }

    fn build(&self, target: &super::Target, options: ResolverOptions) -> Box<dyn Resolver> {
        let id = target.path().strip_prefix("/").unwrap().to_string();

        options.work_scheduler.schedule_work();
        Box::new(NopResolver { id })
    }

    fn is_valid_uri(&self, uri: &super::Target) -> bool {
        true
    }
}

struct NopResolver {
    id: String,
}

impl Resolver for NopResolver {
    fn resolve_now(&mut self) {}

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        let update = ResolverUpdate {
            endpoints: Ok(vec![Endpoint {
                addresses: vec![Address {
                    network_type: TCP_IP_NETWORK_TYPE.to_string(),
                    address: self.id.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            }]),
            ..Default::default()
        };
        let _ = channel_controller.update(update);
    }
}
