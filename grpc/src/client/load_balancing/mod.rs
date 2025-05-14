/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

use core::panic;
use serde::de;
use std::{
    any::Any,
    collections::HashMap,
    error::Error,
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    ops::{Add, Sub},
    sync::{
        atomic::{AtomicI64, Ordering::Relaxed},
        Arc, Mutex, Weak,
    },
};
use tokio::sync::{mpsc::Sender, Notify};
use tonic::{metadata::MetadataMap, Status};

use crate::service::{Request, Response, Service};

use crate::client::{
    name_resolution::{Address, ResolverUpdate},
    subchannel::{ConnectivityStateWatcher, InternalSubchannel},
    ConnectivityState,
};

pub mod child_manager;
pub mod pick_first;

#[cfg(test)]
pub mod test_utils;

mod registry;
use super::service_config::LbConfig;
pub use registry::{LbPolicyRegistry, GLOBAL_LB_REGISTRY};

/// A collection of data configured on the channel that is constructing this
/// LbPolicy.
pub struct LbPolicyOptions {
    /// A hook into the channel's work scheduler that allows the LbPolicy to
    /// request the ability to perform operations on the ChannelController.
    pub work_scheduler: Arc<dyn WorkScheduler>,
}

/// Used to asynchronously request a call into the LbPolicy's work method if
/// the LbPolicy needs to provide an update without waiting for an update
/// from the channel first.
pub trait WorkScheduler: Send + Sync {
    // Schedules a call into the LbPolicy's work method.  If there is already a
    // pending work call that has not yet started, this may not schedule another
    // call.
    fn schedule_work(&self);
}

// Abstract representation of the configuration for any LB policy, stored as
// JSON.  Hides internal storage details and includes a method to deserialize
// the JSON into a concrete policy struct.
#[derive(Debug)]
pub struct ParsedJsonLbConfig(pub serde_json::Value);

impl ParsedJsonLbConfig {
    pub fn convert_to<T: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        let res: T = match serde_json::from_value(self.0.clone()) {
            Ok(v) => v,
            Err(e) => {
                return Err(format!("{}", e).into());
            }
        };
        Ok(res)
    }
}

/// An LB policy factory that produces LbPolicy instances used by the channel
/// to manage connections and pick connections for RPCs.
pub trait LbPolicyBuilder: Send + Sync {
    /// Builds and returns a new LB policy instance.
    ///
    /// Note that build must not fail.  Any optional configuration is delivered
    /// via the LbPolicy's resolver_update method.
    ///
    /// An LbPolicy instance is assumed to begin in a Connecting state that
    /// queues RPCs until its first update.
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicy>;

    /// Reports the name of the LB Policy.
    fn name(&self) -> &'static str;

    /// Parses the JSON LB policy configuration into an internal representation.
    ///
    /// LB policies do not need to accept a configuration, in which case the
    /// default implementation returns Ok(None).
    fn parse_config(
        &self,
        config: &ParsedJsonLbConfig,
    ) -> Result<Option<LbConfig>, Box<dyn Error + Send + Sync>> {
        Ok(None)
    }
}

/// An LB policy instance.
///
/// LB policies are responsible for creating connections (modeled as
/// Subchannels) and producing Picker instances for picking connections for
/// RPCs.
pub trait LbPolicy: Send {
    /// Called by the channel when the name resolver produces a new set of
    /// resolved addresses or a new service config.
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Called by the channel when any subchannel created by the LB policy
    /// changes state.
    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    );

    /// Called by the channel in response to a call from the LB policy to the
    /// WorkScheduler's request_work method.
    fn work(&mut self, channel_controller: &mut dyn ChannelController);
}

/// Controls channel behaviors.
pub trait ChannelController: Send + Sync {
    /// Creates a new subchannel in IDLE state.
    fn new_subchannel(&mut self, address: &Address) -> Arc<dyn Subchannel>;

    /// Provides a new snapshot of the LB policy's state to the channel.
    fn update_picker(&mut self, update: LbState);

    /// Signals the name resolver to attempt to re-resolve addresses.  Typically
    /// used when connections fail, indicating a possible change in the overall
    /// network configuration.
    fn request_resolution(&mut self);
}

/// Represents the current state of a Subchannel.
#[derive(Debug, Clone)]
pub struct SubchannelState {
    /// The connectivity state of the subchannel.  See SubChannel for a
    /// description of the various states and their valid transitions.
    pub connectivity_state: ConnectivityState,
    // Set if connectivity state is TransientFailure to describe the most recent
    // connection error.  None for any other connectivity_state value.
    pub last_connection_error: Option<Arc<dyn Error + Send + Sync>>,
}

impl Default for SubchannelState {
    fn default() -> Self {
        Self {
            connectivity_state: ConnectivityState::Idle,
            last_connection_error: None,
        }
    }
}
impl Display for SubchannelState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "connectivity_state: {}, last_connection_error: {}",
            self.connectivity_state,
            self.last_connection_error
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_default()
        )
    }
}

/// A Picker is responsible for deciding what Subchannel to use for any given
/// request.  A Picker is only used once for any RPC.  If pick() returns Queue,
/// the channel will queue the RPC until a new Picker is produced by the
/// LbPolicy, and will call pick() on the new Picker for the request.
///
/// Pickers are always paired with a ConnectivityState which the channel will
/// expose to applications so they can predict what might happens when
/// performing RPCs:
///
/// If the ConnectivityState is Idle, the Picker should ensure connections are
/// initiated by the LbPolicy that produced the Picker, and return a Queue
/// result so the request is attempted the next time a Picker is produced.
///
/// If the ConnectivityState is Connecting, the Picker should return a Queue
/// result and continue to wait for pending connections.
///
/// If the ConnectivityState is Ready, the Picker should return a Ready
/// Subchannel.
///
/// If the ConnectivityState is TransientFailure, the Picker should return an
/// Err with an error that describes why connections are failing.
pub trait Picker: Send + Sync {
    /// Picks a connection to use for the request.
    ///
    /// This function should not block.  If the Picker needs to do blocking or
    /// time-consuming work to service this request, it should return Queue, and
    /// the Pick call will be repeated by the channel when a new Picker is
    /// produced by the LbPolicy.
    fn pick(&self, request: &Request) -> PickResult;
}

pub enum PickResult {
    /// Indicates the Subchannel in the Pick should be used for the request.
    Pick(Pick),
    /// Indicates the LbPolicy is attempting to connect to a server to use for
    /// the request.
    Queue,
    /// Indicates that the request should fail with the included error status
    /// (with the code converted to UNAVAILABLE).  If the RPC is wait-for-ready,
    /// then it will not be terminated, but instead attempted on a new picker if
    /// one is produced before it is cancelled.
    Fail(Status),
    /// Indicates that the request should fail with the included status
    /// immediately, even if the RPC is wait-for-ready.  The channel will
    /// convert the status code to INTERNAL if it is not a valid code for the
    /// gRPC library to produce, per [gRFC A54].
    ///
    /// [gRFC A54]:
    ///     https://github.com/grpc/proposal/blob/master/A54-restrict-control-plane-status-codes.md
    Drop(Status),
}

impl PickResult {
    pub fn unwrap_pick(self) -> Pick {
        let PickResult::Pick(pick) = self else {
            panic!("Called `PickResult::unwrap_pick` on a `Queue` or `Err` value");
        };
        pick
    }
}

impl PartialEq for PickResult {
    fn eq(&self, other: &Self) -> bool {
        match self {
            PickResult::Pick(pick) => match other {
                PickResult::Pick(other_pick) => pick.subchannel == other_pick.subchannel.clone(),
                _ => false,
            },
            PickResult::Queue => matches!(other, PickResult::Queue),
            PickResult::Fail(status) => {
                // TODO: implement me.
                false
            }
            PickResult::Drop(status) => {
                // TODO: implement me.
                false
            }
        }
    }
}

/// Data provided by the LB policy.
#[derive(Clone)]
pub struct LbState {
    pub connectivity_state: super::ConnectivityState,
    pub picker: Arc<dyn Picker>,
}

impl LbState {
    /// Returns a generic initial LbState which is Connecting and a picker which
    /// queues all picks.
    pub fn initial() -> Self {
        Self {
            connectivity_state: ConnectivityState::Connecting,
            picker: Arc::new(QueuingPicker {}),
        }
    }
}

/// A collection of data used by the channel for routing a request.
pub struct Pick {
    /// The Subchannel for the request.
    pub subchannel: Arc<dyn Subchannel>,
    // Metadata to be added to existing outgoing metadata.
    pub metadata: MetadataMap,
    // Callback to be invoked once the RPC completes.
    pub on_complete: Option<Box<dyn Fn(&Response) + Send + Sync>>,
}

pub trait DynHash {
    fn dyn_hash(&self, state: &mut Box<&mut dyn Hasher>);
}

impl<T: Hash> DynHash for T {
    fn dyn_hash(&self, state: &mut Box<&mut dyn Hasher>) {
        self.hash(state);
    }
}

pub trait DynPartialEq {
    fn dyn_eq(&self, other: &&dyn Any) -> bool;
}

impl<T: Eq + PartialEq + 'static> DynPartialEq for T {
    fn dyn_eq(&self, other: &&dyn Any) -> bool {
        let Some(other) = other.downcast_ref::<T>() else {
            return false;
        };
        self.eq(other)
    }
}

/// A Subchannel represents a method of communicating with a server which may be
/// connected or disconnected many times across its lifetime.
///
/// - Subchannels start IDLE.
///
/// - IDLE transitions to CONNECTING when connect() is called.
///
/// - CONNECTING transitions to READY on success or TRANSIENT_FAILURE on error.
///
/// - READY transitions to IDLE when the connection is lost.
///
/// - TRANSIENT_FAILURE transitions to IDLE when the reconnect backoff timer has
///   expired.  This timer scales exponentially and is reset when the subchannel
///   becomes READY.
///
/// When a Subchannel is dropped, it is disconnected automatically, and no
/// subsequent state updates will be provided for it to the LB policy.
pub trait Subchannel: DynHash + DynPartialEq + Any + Send + Sync {
    /// Returns the address of the Subchannel.
    fn address(&self) -> &Address;

    /// Notifies the Subchannel to connect.
    fn connect(&self);
}

impl dyn Subchannel {
    pub fn downcast_ref<T>(&self) -> Option<&T>
    where
        T: 'static,
    {
        (self as &dyn Any).downcast_ref()
    }
}

impl Hash for dyn Subchannel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(&mut Box::new(state as &mut dyn Hasher));
    }
}

impl PartialEq for dyn Subchannel {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(&Box::new(other as &dyn Any))
    }
}

impl Eq for dyn Subchannel {}

impl Debug for dyn Subchannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subchannel for address: {}", self.address())
    }
}

impl Display for dyn Subchannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subchannel for address: {}", self.address())
    }
}

struct WeakSubchannel(Weak<dyn Subchannel>);

impl WeakSubchannel {
    pub fn new(subchannel: Arc<dyn Subchannel>) -> Self {
        WeakSubchannel(Arc::downgrade(&subchannel))
    }

    pub fn upgrade(&self) -> Option<Arc<dyn Subchannel>> {
        self.0.upgrade()
    }
}

impl Hash for WeakSubchannel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(strong) = self.upgrade() {
            return strong.dyn_hash(&mut Box::new(state as &mut dyn Hasher));
        }
        panic!("WeakSubchannel is not valid");
    }
}

impl PartialEq for WeakSubchannel {
    fn eq(&self, other: &Self) -> bool {
        if let Some(strong) = self.upgrade() {
            return strong.dyn_eq(&Box::new(other as &dyn Any));
        }
        false
    }
}

impl Eq for WeakSubchannel {}

pub(crate) struct SubchannelImpl {
    dropped: Arc<Notify>,
    pub(crate) isc: Arc<InternalSubchannel>,
}

impl SubchannelImpl {
    pub(super) fn new(dropped: Arc<Notify>, isc: Arc<InternalSubchannel>) -> Self {
        SubchannelImpl { dropped, isc }
    }
}

impl Hash for SubchannelImpl {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.isc.address().hash(state);
    }
}

impl PartialEq for SubchannelImpl {
    fn eq(&self, other: &Self) -> bool {
        self.isc.address() == other.isc.address()
    }
}

impl Eq for SubchannelImpl {}

impl Subchannel for SubchannelImpl {
    fn address(&self) -> &Address {
        self.isc.address()
    }

    fn connect(&self) {
        println!("connect called for subchannel: {}", self);
        self.isc.connect(false);
    }
}

impl Drop for SubchannelImpl {
    fn drop(&mut self) {
        println!("dropping subchannel {}", self);
        self.dropped.notify_one();
    }
}

impl Debug for SubchannelImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subchannel {}", self.isc.address())
    }
}

impl Display for SubchannelImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subchannel {}", self.isc.address())
    }
}

/// QueuingPicker always returns Queue.  LB policies that are not actively
/// Connecting should not use this picker.
pub struct QueuingPicker {}

impl Picker for QueuingPicker {
    fn pick(&self, _request: &Request) -> PickResult {
        PickResult::Queue
    }
}

pub struct Failing {
    pub error: String,
}

impl Picker for Failing {
    fn pick(&self, _: &Request) -> PickResult {
        PickResult::Fail(Status::unavailable(self.error.clone()))
    }
}
