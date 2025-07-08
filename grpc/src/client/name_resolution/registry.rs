use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
};

use once_cell::sync::Lazy;
use tokio::sync::Notify;

use super::{Resolver, ResolverBuilder, ResolverOptions};

/// A registry to store and retrieve name resolvers.  Resolvers are indexed by
/// the URI scheme they are intended to handle.
#[derive(Default)]
pub struct ResolverRegistry {
    m: Arc<Mutex<HashMap<String, Arc<dyn ResolverBuilder>>>>,
}

impl ResolverRegistry {
    /// Construct an empty name resolver registry.
    fn new() -> Self {
        Self { m: Arc::default() }
    }

    /// Add a name resolver into the registry. builder.scheme() will
    // be used as the scheme registered with this builder. If multiple
    // resolvers are registered with the same name, the one registered last
    // will take effect. Panics if the given scheme contains uppercase
    // characters.
    pub fn add_builder(&self, builder: Box<dyn ResolverBuilder>) {
        let scheme = builder.scheme();
        if scheme.chars().any(|c| c.is_ascii_uppercase()) {
            panic!("Scheme must not contain uppercase characters: {}", scheme);
        }
        self.m
            .lock()
            .unwrap()
            .insert(scheme.to_string(), Arc::from(builder));
    }

    /// Returns the resolver builder registered for the given scheme, if any.
    ///
    /// The provided scheme is case-insensitive; any uppercase characters
    /// will be converted to lowercase before lookup.
    pub fn get(&self, scheme: &str) -> Option<Arc<dyn ResolverBuilder>> {
        self.m.lock().unwrap().get(&scheme.to_lowercase()).cloned()
    }
}

/// Global registry for resolver builders.
pub static GLOBAL_RESOLVER_REGISTRY: std::sync::LazyLock<ResolverRegistry> =
    std::sync::LazyLock::new(ResolverRegistry::new);
