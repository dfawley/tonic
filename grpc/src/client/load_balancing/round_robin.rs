use crate::client::{
    load_balancing::{
        child_manager::{self, ChildManager, ChildUpdate, ResolverUpdateSharder},
        pick_first::{self},
        ChannelController, ExternalSubchannel, Failing, LbConfig, LbPolicy, LbPolicyBuilder,
        LbPolicyOptions, LbState, ParsedJsonLbConfig, Pick, PickResult, Picker, QueuingPicker,
        Subchannel, SubchannelState, WorkScheduler, GLOBAL_LB_REGISTRY,
    },
    name_resolution::{Address, Endpoint, ResolverUpdate},
    transport::{Transport, GLOBAL_TRANSPORT_REGISTRY},
    ConnectivityState,
};

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    hash::Hash,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::service::{Message, Request, Response, Service};
use core::panic;
use serde_json::json;
use std::{ops::Add, sync::Mutex};
use tokio::{
    sync::{mpsc, Notify},
    task::AbortHandle,
};

use once_cell::sync::Lazy;
use rand::{self, rngs::StdRng, seq::SliceRandom, thread_rng, Rng, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tonic::{async_trait, metadata::MetadataMap};

#[cfg(test)]
mod test;

pub static POLICY_NAME: &str = "round_robin";

struct RoundRobinBuilder {}

impl LbPolicyBuilder for RoundRobinBuilder {
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicy> {
        super::GLOBAL_LB_REGISTRY.add_builder(WrappedPickFirstBuilder {});
        let resolver_update_sharder = ResolverUpdateSharderStruct {
            builder: GLOBAL_LB_REGISTRY.get_policy("wrapped_pick_first").unwrap(),
        };
        let lb_policy = Box::new(ChildManager::new(Box::new(resolver_update_sharder)));
        Box::new(RoundRobinPolicy {
            child_manager: lb_policy,
            work_scheduler: options.work_scheduler,
            addresses: vec![],
            last_resolver_error: None,
            last_connection_error: None,
        })
    }

    fn name(&self) -> &'static str {
        POLICY_NAME
    }
}

/**
Struct for round_robin.
*/
struct RoundRobinPolicy {
    child_manager: Box<ChildManager<Endpoint>>,
    work_scheduler: Arc<dyn WorkScheduler>,
    addresses: Vec<Address>, // Most recent addresses from the name resolver.
    last_resolver_error: Option<String>, // Most recent error from the name resolver.
    last_connection_error: Option<Arc<dyn Error + Send + Sync>>, // Most recent error from any subchannel.
}

impl RoundRobinPolicy {
    fn address_list_from_endpoints(&self, endpoints: &[Endpoint]) -> Vec<Address> {
        // Flatten the endpoints list by concatenating the ordered list of
        // addresses for each of the endpoints.
        let mut addresses: Vec<Address> = endpoints
            .iter()
            .flat_map(|ep| ep.addresses.clone())
            .collect();

        // Remove duplicates.
        let mut uniques = HashSet::new();
        addresses.retain(|e| uniques.insert(e.clone()));

        // TODO(easwars): Implement address family interleaving as part of
        // the dualstack implementation.

        addresses
    }

    fn move_to_transient_failure(&mut self, channel_controller: &mut dyn ChannelController) {
        let err = format!(
            "last seen resolver error: {:?}, last seen connection error: {:?}",
            self.last_resolver_error, self.last_connection_error,
        );
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::TransientFailure,
            picker: Arc::new(Failing { error: err }),
        });
        channel_controller.request_resolution();
    }
}

/// Register round robin as a LbPolicy.
pub fn reg() {
    super::GLOBAL_LB_REGISTRY.add_builder(RoundRobinBuilder {});
}

struct WrapperPickFirstPolicy {
    pick_first: Box<dyn LbPolicy>,
}

struct WrappedPickFirstBuilder {}

pub static WRAPPED_PICKFIRST_NAME: &str = "wrapped_pick_first";

impl LbPolicyBuilder for WrappedPickFirstBuilder {
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicy> {
        pick_first::reg();
        Box::new(WrapperPickFirstPolicy {
            pick_first: GLOBAL_LB_REGISTRY.get_policy("pick_first").unwrap().build(
                LbPolicyOptions {
                    work_scheduler: options.work_scheduler,
                },
            ),
        })
    }

    fn name(&self) -> &'static str {
        WRAPPED_PICKFIRST_NAME
    }
}

impl LbPolicy for WrapperPickFirstPolicy {
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut wrapped_channel_controller = WrappedController::new(channel_controller);
        let result = self
            .pick_first
            .resolver_update(update, None, &mut wrapped_channel_controller);
        if wrapped_channel_controller.idle_update {
            self.exit_idle(&mut wrapped_channel_controller);
        }
        result
    }

    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        let mut wrapped_channel_controller = WrappedController::new(channel_controller);
        self.pick_first
            .subchannel_update(subchannel, state, &mut wrapped_channel_controller);
        if wrapped_channel_controller.idle_update {
            self.exit_idle(&mut wrapped_channel_controller);
        }
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        let mut wrapped_channel_controller = WrappedController::new(channel_controller);
        self.pick_first.work(&mut wrapped_channel_controller);
        if wrapped_channel_controller.idle_update {
            self.exit_idle(&mut wrapped_channel_controller);
        }
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        self.pick_first.exit_idle(channel_controller);
    }
}

struct WrappedController<'a> {
    channel_controller: &'a mut dyn ChannelController,
    idle_update: bool,
}

impl<'a> WrappedController<'a> {
    fn new(channel_controller: &'a mut dyn ChannelController) -> Self {
        Self {
            channel_controller,
            idle_update: false,
        }
    }
}

impl ChannelController for WrappedController<'_> {
    fn new_subchannel(&mut self, address: &Address) -> Arc<dyn Subchannel> {
        let subchannel = self.channel_controller.new_subchannel(address);
        subchannel
    }

    fn update_picker(&mut self, update: LbState) {
        let update_clone = update.clone();
        self.channel_controller.update_picker(update);
        if update_clone.connectivity_state == ConnectivityState::Idle {
            self.idle_update = true;
        } else {
            self.idle_update = false;
        }
    }

    fn request_resolution(&mut self) {
        self.channel_controller.request_resolution();
    }
}

struct ResolverUpdateSharderStruct {
    builder: Arc<dyn LbPolicyBuilder>,
}

impl ResolverUpdateSharder<Endpoint> for ResolverUpdateSharderStruct {
    fn shard_update(
        &self,
        resolver_update: ResolverUpdate,
    ) -> Result<Box<dyn Iterator<Item = ChildUpdate<Endpoint>>>, Box<dyn Error + Send + Sync>> {
        let mut hashmap = HashMap::new();
        for endpoint in resolver_update.endpoints.clone().unwrap().iter() {
            let child_update = ChildUpdate {
                child_identifier: endpoint.clone(),
                child_policy_builder: self.builder.clone(),
                // Create new resolver update with particular endpoint.
                child_update: ResolverUpdate {
                    attributes: resolver_update.attributes.clone(),
                    endpoints: Ok(vec![endpoint.clone()]),
                    service_config: resolver_update.service_config.clone(),
                    resolution_note: resolver_update.resolution_note.clone(),
                },
            };
            hashmap.insert(endpoint.clone(), child_update);
        }
        Ok(Box::new(hashmap.into_values()))
    }
}

impl LbPolicy for RoundRobinPolicy {
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cloned_update = update.clone();
        match update.endpoints {
            Ok(endpoints) => {
                if endpoints.is_empty() {
                    self.last_resolver_error =
                        Some("received no endpoints from the name resolver".to_string());
                    // No endpoints were returned by the resolver.
                    self.move_to_transient_failure(channel_controller);
                    return Err("received no endpoints from the name resolver".into());
                }

                let new_addresses: Vec<Address> = self.address_list_from_endpoints(&endpoints);
                if new_addresses.is_empty() {
                    self.last_resolver_error =
                        Some("received empty address list from the name resolver".to_string());
                    self.move_to_transient_failure(channel_controller);
                    return Err("received empty address list from the name resolver".into());
                }

                let result =
                    self.child_manager
                        .resolver_update(cloned_update, config, channel_controller);

                if self.child_manager.has_updated() {
                    if let Some(pick_update) = self.child_manager.aggregate_states() {
                        channel_controller.update_picker(pick_update);
                    }
                }
                self.addresses = new_addresses;
            }
            Err(error) => {
                self.move_to_transient_failure(channel_controller);
                if self.child_manager.has_updated() {
                    if let Some(pick_update) = self.child_manager.aggregate_states() {
                        channel_controller.update_picker(pick_update);
                    }
                }
            }
        }
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        self.child_manager
            .subchannel_update(subchannel, state, channel_controller);
        if self.child_manager.has_updated() {
            if let Some(pick_update) = self.child_manager.aggregate_states() {
                channel_controller.update_picker(pick_update);
            }
        }
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        // Build a new subchannel list with the most recent addresses received
        // from the name resolver. This will start connecting from the first
        // address in the list.
        self.child_manager.work(channel_controller);
        if self.child_manager.has_updated() {
            if let Some(pick_update) = self.child_manager.aggregate_states() {
                channel_controller.update_picker(pick_update);
            }
        }
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        self.child_manager.exit_idle(channel_controller);
        if self.child_manager.has_updated() {
            if let Some(pick_update) = self.child_manager.aggregate_states() {
                channel_controller.update_picker(pick_update);
            }
        }
    }
}
