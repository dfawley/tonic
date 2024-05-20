use std::{any::Any, error::Error, sync::Arc};
use tonic::{async_trait, metadata::MetadataMap};

use crate::service::{Request, Response};

use super::{
    name_resolution::{Address, ResolverUpdate},
    ConnectivityState,
};

pub mod pick_first;

mod registry;
pub use registry::{LbPolicyRegistry, GLOBAL_LB_REGISTRY};

pub struct TODO;
pub struct LbPolicyOptions {}

/// An LB policy factory
pub trait LbPolicyBuilder: Send + Sync {
    /// Builds an LB policy instance, or returns an error.
    fn build(
        &self,
        subchannel_pool: Arc<dyn SubchannelPool>,
        options: LbPolicyOptions,
    ) -> Box<dyn LbPolicy>;
    /// Reports the name of the LB Policy.
    fn name(&self) -> &'static str;
}

pub trait Picker: Send + Sync {
    fn pick(&self, request: &Request) -> Result<Pick, Box<dyn Error>>;
}

pub type LbUpdate = Result<Box<State>, Box<dyn Error>>;

/// Data provided by the LB policy.
pub struct State {
    pub connectivity_state: super::ConnectivityState,
    pub picker: Box<dyn Picker>,
}

pub struct Pick {
    pub subchannel: Arc<dyn Subchannel>,
    pub on_complete: Option<Box<dyn FnOnce(&Response) + Send + Sync>>,
    pub metadata: Option<MetadataMap>, // to be added to existing outgoing metadata
}

pub struct LbPolicyUpdate {
    pub update: ResolverUpdate,
    pub config: TODO, // LB policy's parsed config
}

#[async_trait]
pub trait LbPolicy: Send + Sync {
    async fn update(&self, update: LbPolicyUpdate) -> Result<(), Box<dyn Error>>;
}

/// Creates and manages subchannels.
pub trait SubchannelPool: Send + Sync {
    /// Creates a new subchannel in idle state.
    fn new_subchannel(&self, address: Arc<Address>) -> Arc<dyn Subchannel>;
    fn update_state(&self, update: LbUpdate);
}

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
