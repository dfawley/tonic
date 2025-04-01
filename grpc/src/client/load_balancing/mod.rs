use std::{
    any::Any,
    collections::HashMap,
    error::Error,
    fmt::Display,
    hash::Hash,
    sync::{
        atomic::{AtomicU32, Ordering::Relaxed},
        Arc,
    },
};
use tokio::sync::{mpsc::Sender, Notify};
use tonic::{async_trait, metadata::MetadataMap};

use crate::service::{Request, Response};

use super::{
    name_resolution::{Address, ResolverUpdate},
    ConnectivityState,
};

pub mod child_manager_batched;
pub mod child_manager_broadcast;
pub mod child_manager_cb;
pub mod child_manager_single;
pub mod pick_first;

mod registry;
pub use registry::{LbPolicyRegistry, GLOBAL_LB_REGISTRY};

pub trait LbPolicyBuilderV2 {
    fn start(&self, cc: Sender<ChannelOperations>) -> Sender<ChannelUpdates>;
}

pub enum ChannelOperations {
    CreateSubchannel(Address),
    ConnectSubchannel(Address),
    RemoveSubchannel(Address),
    UpdatePicker(LbState),
    RequestResolution,
}

pub enum ChannelUpdates {
    NameResolverUpdate(ResolverUpdate),
    SubchannelUpdate(Address, SubchannelState),
}

pub trait LbPolicyV2 {}

pub struct LbPolicyOptions {
    pub work_scheduler: Arc<dyn WorkScheduler>,
}

pub trait WorkScheduler: Send + Sync {
    // Schedules a call into the LbPolicy's work method.  If there is already a
    // pending work call that has not yet started, this may not schedule another
    // call.
    fn schedule_work(&self);
}

/// An LB policy factory
pub trait LbPolicyBuilderSingle: Send + Sync {
    /// Builds an LB policy instance, or returns an error.
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicySingle>;
    /// Reports the name of the LB Policy.
    fn name(&self) -> &'static str;
    // Parses the JSON LB policy configuration into an internal representation.
    fn parse_config(&self, config: &str) -> Result<Option<LbConfig>, Box<dyn Error + Send + Sync>> {
        Ok(None)
    }
}

pub trait LbPolicySingle: Send {
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn subchannel_update(
        &mut self,
        subchannel: &Subchannel,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    );
    fn work(&mut self, channel_controller: &mut dyn ChannelController);
}

/// An LB policy factory
pub trait LbPolicyBuilderBatched: Send + Sync {
    /// Builds an LB policy instance, or returns an error.
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicyBatched>;
    /// Reports the name of the LB Policy.
    fn name(&self) -> &'static str;
    fn parse_config(&self, config: &str) -> Option<LbConfig> {
        None
    }
}

pub trait LbPolicyBatched: Send {
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
    fn subchannel_update(
        &mut self,
        update: &SubchannelUpdate,
        channel_controller: &mut dyn ChannelController,
    );
    fn work(&mut self, channel_controller: &mut dyn ChannelController);
}

/// Controls channel behaviors.
pub trait ChannelController: Send + Sync {
    /// Creates a new subchannel in IDLE state.
    fn new_subchannel(&mut self, address: &Address) -> Subchannel;
    fn update_picker(&mut self, update: LbState);
    fn request_resolution(&mut self);
}

#[derive(Clone)]
pub struct SubchannelState {
    /// The connectivity state of the subchannel.  See SubChannel for a
    /// description of the various states and their valid transitions.
    pub connectivity_state: ConnectivityState,
    // Set if connectivity state is TransientFailure to describe the failure.
    pub last_connection_error: Option<Arc<dyn Error + Send + Sync>>,
}

pub struct SubchannelUpdate {
    pub states: HashMap<Subchannel, SubchannelState>,
}

impl SubchannelUpdate {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }
    pub fn from(u: &SubchannelUpdate) -> Self {
        Self {
            states: u.states.clone().into_iter().collect(),
        }
    }
    pub fn get(&self, subchannel: &Subchannel) -> Option<&SubchannelState> {
        self.states.get(subchannel)
    }
    pub fn set(&mut self, subchannel: &Subchannel, state: SubchannelState) {
        self.states.insert(subchannel.clone(), state);
    }
    pub fn into_iter(self) -> impl Iterator<Item = (Subchannel, SubchannelState)> {
        self.states.into_iter()
    }
    pub fn iter(&self) -> impl Iterator<Item = (&Subchannel, &SubchannelState)> {
        self.states.iter()
    }
}

impl Default for SubchannelUpdate {
    fn default() -> Self {
        Self {
            states: Default::default(),
        }
    }
}

pub struct LbConfig {
    config: Arc<dyn Any>,
}

impl<'a> LbConfig {
    fn new(config: Arc<dyn Any>) -> Self {
        LbConfig { config }
    }

    fn into<T: 'static>(&self) -> Option<&T> {
        self.config.downcast_ref::<T>()
    }
}

pub trait Picker: Send + Sync {
    fn pick(&self, request: &Request) -> PickResult;
}

pub enum PickResult {
    Subchannel(Pick),
    Queue,
    Err(Box<dyn Error + Send + Sync>),
}

impl PickResult {
    pub fn unwrap_pick(self) -> Pick {
        let PickResult::Subchannel(pick) = self else {
            panic!("Called `PickResult::unwrap_pick` on a `Queue` or `Err` value");
        };
        pick
    }
}

/// Data provided by the LB policy.
#[derive(Clone)]
pub struct LbState {
    pub connectivity_state: super::ConnectivityState,
    pub picker: Arc<dyn Picker>,
}

impl LbState {
    // Returns an generic initial state which is Connecting and a picker which
    // queues all picks.
    pub fn initial() -> Self {
        Self {
            connectivity_state: ConnectivityState::Connecting,
            picker: Arc::new(QueuingPicker {}),
        }
    }
}

pub struct Pick {
    pub subchannel: Subchannel,
    pub on_complete: Option<Box<dyn Fn(&Response) + Send + Sync>>,
    pub metadata: Option<MetadataMap>, // to be added to existing outgoing metadata
}

/// A Subchannel represents a method of communicating with an address which may
/// be connected or disconnected many times across its lifetime.
///
/// - Subchannels start IDLE and transition to CONNECTING when connect is
///   called.
///
/// - CONNECTING leads to either READY on success or TRANSIENT_FAILURE on error.
///
/// - READY transitions to IDLE when the connection is lost.
///
/// - TRANSIENT_FAILURE transitions to CONNECTING when the reconnect backoff
///   timer has expired.  This timer scales exponentially and is reset when the
///   subchannel becomes READY.
///
/// When a Subchannel is dropped, it is disconnected automatically, and no
/// subsequent state updates will be provided for it to the LB policy.
#[derive(Clone, Debug)]
pub struct Subchannel {
    id: u32,
    notify: Arc<Notify>,
}

impl Display for Subchannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subchannel {}", self.id)
    }
}

static NEXT_SUBCHANNEL_ID: AtomicU32 = AtomicU32::new(0);

impl Subchannel {
    /// Creates a new Subchannel that doesn't do anything besides forward
    /// connect calls to notify.
    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            id: NEXT_SUBCHANNEL_ID.fetch_add(1, Relaxed),
            notify,
        }
    }
    /// Notifies the Subchannel to connect.
    pub fn connect(&self) {
        self.notify.notify_one();
    }
}

impl Hash for Subchannel {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Subchannel {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Subchannel {}

pub trait LbPolicyBuilderCallbacks: Send {
    /// Builds an LB policy instance, or returns an error.
    fn build(
        &self,
        options: LbPolicyOptions,
        channel_controller: Arc<dyn ChannelControllerCallbacks>,
    ) -> Box<dyn LbPolicyCallbacks>;
    /// Reports the name of the LB Policy.
    fn name(&self) -> &'static str;
    fn parse_config(&self, config: &str) -> Option<LbConfig> {
        None
    }
}

pub trait LbPolicyCallbacks: Send {
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub type SubchannelUpdateFn = Box<dyn Fn(Subchannel, SubchannelState) + Send + Sync>;

// May only be used at synchronization points -- when called from the work
// scheduler or in a call to resolver_update or the SubchannelUpdateFn on a
// subchannel.
pub trait ChannelControllerCallbacks: Send + Sync {
    /// Creates a new subchannel in IDLE state.
    fn new_subchannel(&self, address: &Address, updates: SubchannelUpdateFn) -> Subchannel;
    fn update_picker(&self, update: LbState);
    fn request_resolution(&self);
}

pub struct QueuingPicker {}

impl Picker for QueuingPicker {
    fn pick(&self, request: &Request) -> PickResult {
        PickResult::Queue
    }
}
