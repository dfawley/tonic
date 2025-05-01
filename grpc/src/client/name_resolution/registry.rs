use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use once_cell::sync::Lazy;
use tokio::sync::Notify;
use url::Url;

use super::{Resolver, ResolverBuilder, ResolverOptions};

#[derive(Clone)]
pub struct SharedResolverBuilder {
    rb: Arc<dyn ResolverBuilder>,
}

impl SharedResolverBuilder {
    pub fn new(rb: impl ResolverBuilder + 'static) -> Self {
        Self { rb: Arc::new(rb) }
    }
}

impl ResolverBuilder for SharedResolverBuilder {
    fn scheme(&self) -> &'static str {
        self.rb.scheme()
    }

    fn build(
        &self,
        target: Url,
        resolve_now: Arc<Notify>,
        options: ResolverOptions,
    ) -> Box<dyn Resolver> {
        self.rb.build(target, resolve_now, options)
    }
}

/// A registry to store and retrieve name resolvers.  Resolvers are indexed by
/// the URI scheme they are intended to handle.
pub struct ResolverRegistry {
    m: Arc<Mutex<HashMap<String, SharedResolverBuilder>>>,
}

impl ResolverRegistry {
    /// Construct an empty name resolver registry.
    pub fn new() -> Self {
        Self { m: Arc::default() }
    }
    /// Add a name resolver into the registry.
    pub fn add_builder(&self, builder: SharedResolverBuilder) {
        self.m
            .lock()
            .unwrap()
            .insert(builder.scheme().to_string(), builder);
    }
    /// Retrieve a name resolver from the registry, or None if not found.
    pub fn get_scheme(&self, name: &str) -> Option<SharedResolverBuilder> {
        self.m.lock().unwrap().get(name).cloned()
    }
}

impl Default for ResolverRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// The registry used if a local registry is not provided to a channel or if it
/// does not exist in the local registry.
pub static GLOBAL_RESOLVER_REGISTRY: Lazy<ResolverRegistry> = Lazy::new(ResolverRegistry::new);
