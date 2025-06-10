use crate::client::{
    load_balancing::{
        pick_first::{
            self, 
        },
        child_manager::{
            self, 
        },
        ChannelController, ExternalSubchannel, Failing, LbConfig, LbPolicy, LbPolicyBuilder,
        LbPolicyOptions, LbState, ParsedJsonLbConfig, PickResult, Picker, QueuingPicker,
        Subchannel, SubchannelState, WorkScheduler, GLOBAL_LB_REGISTRY,
    },
    name_resolution::{Address, Endpoint, ResolverUpdate},
    transport::{Transport, GLOBAL_TRANSPORT_REGISTRY},
    ConnectivityState,
};

use std::{collections::{HashMap, HashSet}, error::Error, hash::Hash, mem, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

use crate::service::{Message, Request, Response, Service};
use core::panic;
use serde_json::json;
use std::{
    ops::Add,
    sync::{ Mutex},
};
use tokio::{
    sync::{mpsc, Notify},
    task::AbortHandle,
};

use crate::client::load_balancing::child_manager::ChildManager;

use once_cell::sync::Lazy;
use rand::{self, rngs::StdRng, seq::SliceRandom, thread_rng, Rng, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tonic::{async_trait, metadata::MetadataMap};
use crate::client::load_balancing::child_manager::ChildUpdate;

use crate::client::load_balancing::child_manager::ResolverUpdateSharder;
use crate::client::load_balancing::Pick;

#[cfg(test)]
mod test;

pub static POLICY_NAME: &str = "round_robin";

struct RoundRobinBuilder {}

impl LbPolicyBuilder for RoundRobinBuilder {
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicy> {
        super::GLOBAL_LB_REGISTRY.add_builder(WrappedPickFirstBuilder {});
        let resolver_update_sharder = ResolverUpdateSharderStruct {builder:GLOBAL_LB_REGISTRY.get_policy("wrapped_pick_first").unwrap()};
        let lb_policy = Box::new(ChildManager::<Endpoint>::new(options.work_scheduler.clone(), Box::new(resolver_update_sharder)));
        Box::new(RoundRobinPolicy {
            child_manager: lb_policy,
            work_scheduler: options.work_scheduler,
            addresses: vec![],
            last_resolver_error: None,
            last_connection_error: None,
            connectivity_state: ConnectivityState::Idle,
            sent_connecting_state: false,
        })
    }

    fn name(&self) -> &'static str {
        POLICY_NAME
    }

}

/** 
struct for round_robin
*/
//wrap pick first in a LB policy that calls ExitIdle whenever a subchannel disconnects
struct RoundRobinPolicy {

    //builder or built?
    child_manager: Box<ChildManager<Endpoint>>,
    work_scheduler: Arc<dyn WorkScheduler>,
    addresses: Vec<Address>,                 // Most recent addresses from the name resolver.
    last_resolver_error: Option<String>,     // Most recent error from the name resolver.
    last_connection_error: Option<Arc<dyn Error + Send + Sync>>, // Most recent error from any subchannel.
    connectivity_state: ConnectivityState, // Overall connectivity state of the channel.
    sent_connecting_state:bool,
    // subchannels: Vec<Arc <dyn Subchannel>>,
    // num_transient_failures: usize, // Number of transient failures after the end of the first pass.
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
        self.connectivity_state = ConnectivityState::TransientFailure;
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



    fn move_to_connecting(&mut self, is_idle: bool, channel_controller: &mut dyn ChannelController) {
        self.connectivity_state = ConnectivityState::Connecting;
        println!("sending a picker with connecting");
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::Connecting,
            picker: Arc::new(QueuingPicker {}),
        });
        self.sent_connecting_state = true;
        if is_idle {
            channel_controller.request_resolution();
        }
    }


}

struct RoundRobinPicker {
    pickers: Vec<Arc<dyn Picker>>,
    next: AtomicUsize,
}

impl RoundRobinPicker {
    pub fn new(pickers: Vec<Arc<dyn Picker>>) -> Self {
        Self {
            pickers,
            next: AtomicUsize::new(0),
        }
    }
}

impl Picker for RoundRobinPicker {
    fn pick(&self, request: &Request) -> PickResult {
        let len = self.pickers.len();
        if len == 0 {
            return PickResult::Queue;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % len;
        self.pickers[idx].pick(request)
    }
}

pub fn reg() {
    super::GLOBAL_LB_REGISTRY.add_builder(RoundRobinBuilder {});
}

//have to instantiate pickfirst builder first
struct WrapperPickFirstPolicy {pick_first: Box<dyn LbPolicy>}

struct WrappedPickFirstBuilder {}

pub static WRAPPED_PICKFIRST_NAME: &str = "wrapped_pick_first";

impl LbPolicyBuilder for WrappedPickFirstBuilder{
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicy> {
        Box::new(WrapperPickFirstPolicy {
            pick_first: GLOBAL_LB_REGISTRY.get_policy("pick_first").unwrap().build(LbPolicyOptions { work_scheduler: options.work_scheduler}),
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
        
        let result = self.pick_first.resolver_update(update, config, &mut wrapped_channel_controller);
        if let Some(state) = wrapped_channel_controller.picker_update.clone() {
            if state.connectivity_state == ConnectivityState::Idle {
                    self.exit_idle(&mut wrapped_channel_controller);
            }
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
        self.pick_first.subchannel_update(subchannel, state, &mut wrapped_channel_controller);
        if let Some(state) = wrapped_channel_controller.picker_update.clone() {
            if state.connectivity_state == ConnectivityState::Idle {
                self.exit_idle(&mut wrapped_channel_controller);
            }

        }
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        let mut wrapped_channel_controller = WrappedController::new(channel_controller);
    
        self.pick_first.work(&mut wrapped_channel_controller);
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        self.pick_first.exit_idle(channel_controller);
    }
}

struct WrappedController<'a> {
    channel_controller: &'a mut dyn ChannelController,
    // created_subchannels: Vec<Arc<dyn Subchannel>>,

    //have this be a bool and set true when set to idle
    picker_update: Option<LbState>,
}

impl<'a> WrappedController<'a> {
    fn new(channel_controller: &'a mut dyn ChannelController) -> Self {
        Self {
            channel_controller,
            // created_subchannels: vec![],
            picker_update: None, //change into bool
        }
    }
}

impl ChannelController for WrappedController<'_> {
    //call into the real channel controller
    fn new_subchannel(&mut self, address: &Address) -> Arc<dyn Subchannel> {
        let subchannel = self.channel_controller.new_subchannel(address);
        subchannel
    }

    fn update_picker(&mut self, update: LbState) {
        self.channel_controller.update_picker(update);
        // self.picker_update = Some(update);
    }

    fn request_resolution(&mut self) {
        // self.channel_controller.request_resolution(update);
        self.channel_controller.request_resolution();
    }
}

//build pick first builder in round robin builder and get policy (if not seen, panic)
struct ResolverUpdateSharderStruct {builder: Arc<dyn LbPolicyBuilder>}

//need implementation for sharder trait and pass it to child manager new in build
//and store it in round robin policy. need another struct to implement this trait.
//need to pass an instance of that trait when creating child manager. then store that child manager
//in round robin policy. 
impl ResolverUpdateSharder<Endpoint> for ResolverUpdateSharderStruct {
    fn shard_update(
        &self,
        resolver_update: ResolverUpdate,
        //T is an endpoint and ChildUpdate struct is going to have a builder
        //pass in pickfirst into that builder
        //child update field in the ChildUpdate struct 
        //would contain any attributes in the input resolverupdate
    ) -> Result<HashMap<Endpoint, ChildUpdate>, Box<dyn Error + Send + Sync>> {
        let mut hashmap = HashMap::new();
        let builder = self.builder.clone();
        for endpoint in resolver_update.endpoints.clone().unwrap().iter() {
            let child_update = ChildUpdate{
                child_policy_builder: self.builder.clone(),
                //create new resolver update with particular endpoint
                child_update: ResolverUpdate {attributes: resolver_update.attributes.clone(), endpoints: Ok(vec![endpoint.clone()]), service_config: resolver_update.service_config.clone(), resolution_note: resolver_update.resolution_note.clone()},
            };
            hashmap.insert(endpoint.clone(), child_update);
        }
        Ok(hashmap)
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
            Ok( endpoints) => {
                //create child manager here?

                println!(
                    "received update from resolver with endpoints: {:?}",
                    endpoints
                );
                if endpoints.is_empty() {
                    self.last_resolver_error = Some("received empty address list from the name resolver".to_string());
                    // No endpoints were returned by the resolver.
                    
                    self.move_to_transient_failure(channel_controller);
                    return Err("received empty address list from the name resolver".into());
                }
                let new_addresses: Vec<Address> = self.address_list_from_endpoints(&endpoints);
                if new_addresses.is_empty(){
                    self.last_resolver_error = Some("received empty address list from the name resolver".to_string());
                    
                    self.move_to_transient_failure(channel_controller);
                    return Err("received empty address list from the name resolver".into());
                }
                let result = self.child_manager.resolver_update(cloned_update, config, channel_controller);
                self.addresses = new_addresses;

               
            }
            //think about whether to handle here or in child manager
            Err(error) => {
                println!("received error from resolver: {}", error);
                // self.last_resolver_error = Some(error);

                // self.move_to_transient_failure(channel_controller);
                // if self.addresses.is_empty()
                //     || self.connectivity_state == ConnectivityState::TransientFailure
                // {
                println!("moving to transient failure");
                self.move_to_transient_failure(channel_controller);
                // }

                // Continue using the previous good update, if one exists.
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
        println!("received update for {}: {}", subchannel, state);

        self.child_manager.subchannel_update(subchannel, state, channel_controller);
        // let child_states_vec: Vec<_> = self.child_manager.child_states().collect();

        // let mut found_ready = false;
        // let mut found_connecting_or_idle = false;
        // let mut found_idle = false;

        // for (_, state) in &child_states_vec {
        //     match state.connectivity_state {
        //         ConnectivityState::Idle => {
        //             found_idle = true;
        //             found_connecting_or_idle = true;
        //             break;
        //         }
        //         ConnectivityState::Connecting  => {
        //             found_connecting_or_idle = true;
        //             break;
        //         }
        //         ConnectivityState::Ready => {
        //             found_ready = true;
        //             break;
        //         }
                
        //         _ => {}
        //     }
        // }
        // if !found_ready{

        // }

        // let prev_state = self.connectivity_state;
        // let new_state = if found_ready {
        //     ConnectivityState::Ready
        // } else if found_connecting_or_idle {
        //     ConnectivityState::Connecting
        // } else {
        //     ConnectivityState::TransientFailure
        // };

        
        // self.connectivity_state = new_state;
        // match new_state {
        //     ConnectivityState::TransientFailure => {
        //         println!("state is transient failure");
        //         self.move_to_transient_failure(channel_controller);
        //     }
        //     ConnectivityState::Ready => {
        //         println!("state is Ready");
        //         let ready_pickers = child_states_vec
        //             .iter()
        //             .filter(|(_, state)| state.connectivity_state == ConnectivityState::Ready)
        //             .map(|(_, state)| state.picker.clone())
        //             .collect();

        //         let picker = Arc::new(RoundRobinPicker::new(ready_pickers));
        //         channel_controller.update_picker(LbState {
        //             connectivity_state: self.connectivity_state,
        //             picker,
        //         });
        //         self.sent_connecting_state = false;
        //     }
        //     ConnectivityState::Connecting => {
        //         println!("state is connecting");
        //         if !self.sent_connecting_state{
        //             self.move_to_connecting(found_idle, channel_controller);
        //         }
        //     }
        //     ConnectivityState::Idle => {
        //         println!("state is idle so connect");
        //         if !self.sent_connecting_state{
        //             self.move_to_connecting(found_idle, channel_controller);
        //         }
                
        //     }
        // }
        
    // }
        
    }
   
    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        // Build a new subchannel list with the most recent addresses received
        // from the name resolver. This will start connecting from the first
        // address in the list.

        self.child_manager.work(channel_controller);

        // self.subchannel_list = Some(SubchannelList::new(&self.addresses, channel_controller));
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
 

        self.child_manager.exit_idle(channel_controller);

    }
}

pub struct RoundRobinQueuingPicker {
    work_scheduler: Arc<dyn WorkScheduler>,
}

impl Picker for RoundRobinQueuingPicker {
    fn pick(&self, _request: &Request) -> PickResult {
        self.work_scheduler.schedule_work();
        PickResult::Queue
    }
}