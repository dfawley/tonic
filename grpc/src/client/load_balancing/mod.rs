use once_cell::sync::Lazy;
use std::{
    any::Any,
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
};
use tonic::{async_trait, metadata::MetadataMap};

use crate::service::{Message, Request, Response};

use super::{
    name_resolution::{Address, ResolverUpdate},
    ConnectivityState,
};

pub mod pick_first;

pub struct TODO;

/// A registry to store and retrieve LB policies.  LB policies are indexed by
/// their names.
pub struct Registry {
    m: Arc<Mutex<HashMap<String, Arc<dyn Builder>>>>,
}

impl Registry {
    /// Construct an empty LB policy registry.
    pub fn new() -> Self {
        Self {
            m: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    /// Add a LB policy into the registry.
    pub fn add_builder(&self, builder: impl Builder + 'static) {
        self.m
            .lock()
            .unwrap()
            .insert(builder.name().to_string(), Arc::new(builder));
    }
    /// Retrieve a LB policy from the registry, or None if not found.
    pub fn get_policy(&self, name: &str) -> Option<Arc<dyn Builder>> {
        self.m.lock().unwrap().get(name).map(|f| f.clone())
    }
}

/// The registry used if a local registry is not provided to a channel or if it
/// does not exist in the local registry.
pub static GLOBAL_REGISTRY: Lazy<Registry> = Lazy::new(|| Registry::new());

pub trait Subchannel: Send + Sync {
    /// Begins connecting the subchannel.
    fn connect(&self);
    // Attaches a listener to the subchannel.  Must be called before connect and
    // not after connect.
    fn listen(
        &self,
        updates: Box<dyn Fn(ConnectivityState) + Send + Sync>, // TODO: stream/asynciter/channel probably
    );
    fn shutdown(&self);
    fn as_any(&self) -> &dyn Any;
}

/// An LB policy factory
pub trait Builder: Send + Sync {
    /// Builds an LB policy instance, or returns an error.
    fn build(&self, channel: Box<dyn Channel>, options: TODO) -> Box<dyn Policy>;
    /// Reports the name of the LB Policy.
    fn name(&self) -> &'static str;
}

pub type Update = Result<Box<State>, Box<dyn Error>>;
pub type Picker = dyn Fn(&Request<Box<dyn Message>>) -> Result<Pick, Box<dyn Error>> + Send + Sync;

/// Data provided by the LB policy.
pub struct State {
    pub connectivity_state: super::ConnectivityState,
    pub picker: Box<Picker>,
}

pub struct Pick {
    pub subchannel: Arc<dyn Subchannel>,
    pub on_complete: Option<Box<dyn FnOnce(Response<Box<dyn Message>>) + Send + Sync>>,
    pub metadata: Option<MetadataMap>, // to be added to existing outgoing metadata
}

pub struct PolicyUpdate {
    pub update: ResolverUpdate,
    pub config: TODO, // LB policy's parsed config
}

#[async_trait]
pub trait Policy: Send + Sync {
    async fn update(&self, update: PolicyUpdate);
}

/// This channel is a set of features the LB policy may use from the channel.
pub trait Channel: Send + Sync {
    /// Creates a new subchannel in idle state.
    fn new_subchannel(&self, address: Arc<Address>) -> Arc<dyn Subchannel>;
    /// Consumes an update from the LB Policy.
    fn update_state(&self, update: Update);
}
