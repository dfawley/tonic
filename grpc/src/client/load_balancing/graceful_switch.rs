use crate::client::{
    channel::{InternalChannelController, WorkQueueItem, WorkQueueTx}, load_balancing::{
        child_manager::{
            self, 
        }, pick_first::{
            self, 
        }, ChannelController, ExternalSubchannel, Failing, LbConfig, LbPolicy, LbPolicyBuilder, LbPolicyOptions, LbState, ParsedJsonLbConfig, PickResult, Picker, QueuingPicker, Subchannel, SubchannelState, WeakSubchannel, WorkScheduler, GLOBAL_LB_REGISTRY
    }, name_resolution::{Address, Endpoint, ResolverUpdate}, transport::{Transport, GLOBAL_TRANSPORT_REGISTRY}, ConnectivityState
    
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


pub static POLICY_NAME: &str = "round_robin";

#[cfg(test)]
mod test;

// impl WorkScheduler for GracefulSwitchPolicy {
//     fn schedule_work(&self) {
//         if mem::replace(&mut *self.pending.lock().unwrap(), true) {
//             // Already had a pending call scheduled.
//             return;
//         }
//         let _ = self.work_scheduler.send(WorkQueueItem::Closure(Box::new(
//             |c: &mut InternalChannelController| {
//                 *c.lb.pending.lock().unwrap() = false;
//                 c.lb.clone()
//                     .policy
//                     .lock()
//                     .unwrap()
//                     .as_mut()
//                     .unwrap()
//                     .work(c);
//             },
//         )));
//     }
// }

#[derive(Deserialize)]
pub(super) struct GracefulSwitchConfig {
    children_policies: Vec<HashMap<String, serde_json::Value>>,
}

pub(super) struct GracefulSwitchLbConfig {
    child_builder: Arc<dyn LbPolicyBuilder>,
    child_config: Option<LbConfig>,
}

impl GracefulSwitchLbConfig{
    fn new(child_builder: Arc<dyn LbPolicyBuilder>, child_config: Option<LbConfig>) -> Self{
        GracefulSwitchLbConfig{
            child_builder,
            child_config,
        }
    }
}
/** 
struct for round_robin
*/
//wrap pick first in a LB policy that calls ExitIdle whenever a subchannel disconnects
struct GracefulSwitchPolicy {
    // mutex: Mutex<Arc<dyn LbPolicy>>,
    // current_mutex: Mutex<Arc<dyn LbPolicy>>,
    current_policy_builder: Mutex<Option<Arc<dyn LbPolicyBuilder>>>,
    pending_policy_builder: Mutex<Option<Arc<dyn LbPolicyBuilder>>>,
    subchannel_to_policy: HashMap<WeakSubchannel, ChildKind>,
    current_policy: Mutex<Option<Box<dyn LbPolicy>>>,
    pending_policy: Mutex<Option<Box<dyn LbPolicy>>>,
    current_policy_state: Mutex<Option<ConnectivityState>>,
    pending_policy_state: Mutex<Option<ConnectivityState>>,
    closed: bool,
    work_scheduler: Arc<dyn WorkScheduler>,
    pending: bool,
    
}

impl LbPolicy for GracefulSwitchPolicy{
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if update.service_config.as_ref().is_ok_and(|sc| sc.is_some()) {
            return Err("can't do service configs yet".into());
        }
        let cfg: Arc<GracefulSwitchLbConfig> = match config.unwrap().convert_to::<Arc<GracefulSwitchLbConfig>>() {
            Ok(cfg) => (*cfg).clone(),
            Err(e) => panic!("convert_to failed: {e}"),
        };
        let new_builder_name = cfg.child_builder.name();

        // Get the latest builder name (pending if exists, else current)
        let mut latest_builder_name = "none";
        if let Some(pending) = self.pending_policy_builder.lock().unwrap().as_ref() {
            latest_builder_name = pending.name();
        } else if let Some(current) = self.current_policy_builder.lock().unwrap().as_ref() {
            latest_builder_name = current.name();
        } else {
            latest_builder_name = "";
        };
        let mut child_kind_to_update = self.latest_balancer();
        // Determine which policy to update and whether it is None, but drop the lock before further mutable borrows.
        let (policy_to_update_none, child_kind_to_update_copy) = {
            let policy_to_update = match child_kind_to_update {
                ChildKind::Current => &self.current_policy,
                ChildKind::Pending => &self.pending_policy,
            };
            let is_none = {
                let current_policy = policy_to_update.lock().unwrap();
                current_policy.is_none()
            };
            (is_none, child_kind_to_update.clone())
        };

        let mut wrapped_channel_controller = WrappedController::new(channel_controller);
        let update_clone = update.clone();

        if policy_to_update_none || latest_builder_name != new_builder_name{
            let child_kind = self.switch_to(update, config, child_kind_to_update.clone());
            child_kind_to_update = child_kind;
        }
        let actual_child_to_update = child_kind_to_update.clone();

        if let Some(pending) = self.pending_policy_builder.lock().unwrap().as_ref() {
            println!("pending policy is {}", pending.name());
        }
        if let Some(current) = self.current_policy_builder.lock().unwrap().as_ref() {
            println!("current policy is {}", current.name());
        }

        {
            let policy_to_update = match actual_child_to_update {
                ChildKind::Current => &self.current_policy,
                ChildKind::Pending => &self.pending_policy,
            };
            let state_to_update = match actual_child_to_update {
                ChildKind::Current => &self.current_policy_state,
                ChildKind::Pending => &self.pending_policy_state,
            };
            println!("Sending resolver_update to {:?}", actual_child_to_update);
            if let Some(ref mut policy) = *policy_to_update.lock().unwrap() {
                policy.resolver_update(update_clone, cfg.child_config.as_ref(), &mut wrapped_channel_controller)?;
            }
            let picker_update = wrapped_channel_controller.picker_update.clone();
            if let Some(picker) = picker_update {
                *state_to_update.lock().unwrap() = Some(picker.connectivity_state);
            }
        }
        let current_state = self.current_policy_state.lock().unwrap().clone();
        let pending_state = self.pending_policy_state.lock().unwrap().clone();
        let mut should_swap = false;
        match (current_state, pending_state) {
            (Some(curr), Some(pend)) => {
                if curr != ConnectivityState::Ready || pend != ConnectivityState::Connecting {
                    println!("should swap");
                    should_swap = true;
                }
            }
        _ => {
            should_swap = false;
            }
        }

        if should_swap {
            println!("swapping");
            self.swap();
        }
       
        self.resolve_child_controller(&mut wrapped_channel_controller, child_kind_to_update);
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        let mut wrapped_channel_controller = WrappedController::new(channel_controller);
        let which_child = self
            .subchannel_to_policy
            .get(&WeakSubchannel::new(subchannel.clone()))
            .unwrap_or_else(|| {
                panic!("Subchannel not found in child manager: {}", subchannel);
            }).clone(); // Clone the ChildKind
        // let current_policy_state = 
        

        match which_child {
            ChildKind::Pending => {
                println!("sending subchannel update to pending kid");
                if let Some(ref mut pending_policy) = *self.pending_policy.lock().unwrap() {
                    pending_policy.subchannel_update(subchannel, state, &mut wrapped_channel_controller);
                    let picker_update = wrapped_channel_controller.picker_update.clone();
                    if let Some(picker) = picker_update {
                        *self.pending_policy_state.lock().unwrap() = Some(picker.connectivity_state);
                    }
                   
                }
            }
            ChildKind::Current => {
                println!("sending subchannel update to current kid");
                if let Some(ref mut current_policy) = *self.current_policy.lock().unwrap() {
                    
                    current_policy.subchannel_update(subchannel, state, &mut wrapped_channel_controller);
                    let picker_update = wrapped_channel_controller.picker_update.clone();
                    if let Some(picker) = picker_update {
                        *self.current_policy_state.lock().unwrap() = Some(picker.connectivity_state);
                    }

                }
            }
        }

        let current_state = self.current_policy_state.lock().unwrap().clone();
        let pending_state = self.pending_policy_state.lock().unwrap().clone();

        
        let mut should_swap = false;
        match (current_state, pending_state) {
            (Some(curr), Some(pend)) => {
                if curr != ConnectivityState::Ready || pend != ConnectivityState::Connecting {
                    println!("should swap");
                    should_swap = true;
                }
            }
        _ => {
            should_swap = false;
        }
    }

        if should_swap {
            println!("swapping");
            // Promote pending to current
            self.swap();
            // Optionally clear subchannel_to_policy for old current, etc.
        }
        if let Some(pending) = self.pending_policy_builder.lock().unwrap().as_ref() {
            println!("pending policy is {}", pending.name());
        }
        if let Some(current) = self.current_policy_builder.lock().unwrap().as_ref() {
            println!("current policy is {}", current.name());
        }

    // self.resolve_child_controller(&mut wrapped_channel_controller, which_child);
        self.resolve_child_controller(&mut wrapped_channel_controller, which_child);
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        if let Some(ref mut pending_policy) = *self.pending_policy.lock().unwrap() {
            pending_policy.work(channel_controller);
        } else if let Some(ref mut current_policy) = *self.current_policy.lock().unwrap() {
            current_policy.work(channel_controller);
        }
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        if let Some(ref mut pending_policy) = *self.pending_policy.lock().unwrap() {
            pending_policy.exit_idle(channel_controller);
        } else if let Some(ref mut current_policy) = *self.current_policy.lock().unwrap() {
            current_policy.exit_idle(channel_controller);
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum ChildKind {
    Current,
    Pending,
}

impl GracefulSwitchPolicy {
    pub fn new(
        work_scheduler: Arc<dyn WorkScheduler>,
    ) -> Self {
        GracefulSwitchPolicy { 
            // mutex: Mutex<Arc<dyn LbPolicy>>,
            // current_mutex: Mutex<Arc<dyn LbPolicy>>,
            current_policy_builder: Mutex::default(),
            pending_policy_builder: Mutex::default(),
            subchannel_to_policy: HashMap::default(),
            current_policy: Mutex::default(),
            pending_policy: Mutex::default(),
            current_policy_state: Mutex::new(None), 
            pending_policy_state: Mutex::new(None),
            closed: false,
            work_scheduler: work_scheduler,
            pending: false,
        }
    }

    fn resolve_child_controller(
        &mut self,
        channel_controller: &mut WrappedController,        
        child_kind: ChildKind,
    ) {
        // Add all created subchannels into the subchannel_child_map.
        for csc in channel_controller.created_subchannels.clone() {
            let key = WeakSubchannel::new(csc.clone());
            if !self.subchannel_to_policy.contains_key(&key) {
                // println!("inserting subchannel {} into child_id {}", csc, child);
                self.subchannel_to_policy.insert(key, child_kind.clone());
                // }
            }
        }
    }

    fn swap(&mut self){
        let mut current_policy = self.current_policy.lock().unwrap();
        let mut pending_policy = self.pending_policy.lock().unwrap();

        self.subchannel_to_policy.retain(|_, v| *v != ChildKind::Current);
        // HashMap<WeakSubchannel, ChildKind>
        let mut new_map: HashMap<WeakSubchannel, ChildKind> = HashMap::new();
        // self.subchannel_to_policy.clear();
        // for v in self.subchannel_to_policy.values_mut() {
        //     if *v == ChildKind::Pending {
        //         WeakSubchannel::new(self.subchannel_to_policy.get(clone());
        //         new_map.insert();
        //     }
        // }
        // self.current_policy = Mutex::new(current_policy);
        *current_policy = pending_policy.take();
        *self.current_policy_state.lock().unwrap() = self.pending_policy_state.lock().unwrap().take();
        
        let mut pending_policy_builder = self.pending_policy_builder.lock().unwrap();
        let mut current_policy_builder = self.current_policy_builder.lock().unwrap();
        *current_policy_builder = pending_policy_builder.take();
        *pending_policy_builder = None;
        
    }

    fn parse_config(
        config: &ParsedJsonLbConfig,
    ) -> Result<Option<LbConfig>, Box<dyn Error + Send + Sync>> {
        let cfg: GracefulSwitchConfig = match config.convert_to() {
            Ok(c) => c,
            Err(e) => {
                return Err(format!("failed to parse JSON config: {}", e).into());
            }
        };
        for c in &cfg.children_policies {
            assert!(
                c.len() == 1,
                "Each children_policies entry must contain exactly one policy, found {}",
                c.len()
            );
            if let Some((policy_name, policy_config)) = c.iter().next() {
                if let Some(child) = GLOBAL_LB_REGISTRY.get_policy(policy_name.as_str()) {
                    println!("policy name that currently on is {}", policy_name);
                    if policy_name == "round_robin"{
                        println!("is round robin");
                        let graceful_switch_lb_config = GracefulSwitchLbConfig::new(child, None);
                        return Ok(Some(LbConfig::new(Arc::new(graceful_switch_lb_config))))
                    }
                    let parsed_config = ParsedJsonLbConfig(policy_config.clone());
                    let config_result = child.parse_config(&parsed_config);
                    let config = match config_result {
                        Ok(Some(cfg)) => cfg,
                        Ok(None) => {
                            return Err("child policy config returned None".into());
                        }
                        Err(e) => {
                            println!("returning error in parse_config");
                            return Err(format!("failed to parse child policy config: {}", e).into());
                        }
                    };
                    println!("child name in parse_config is {}", child.name());
                    let graceful_switch_lb_config = GracefulSwitchLbConfig::new(child, Some(config));
                    return Ok(Some(LbConfig::new(Arc::new(graceful_switch_lb_config))))
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
        }
        Ok(None)
    }
    //how do i set pending child to the child from parse_config?

    fn switch_to (&mut self, 
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        latest_balancer_kind: ChildKind) -> ChildKind {
        let cfg: Arc<GracefulSwitchLbConfig> = match config.unwrap().convert_to::<Arc<GracefulSwitchLbConfig>>() {
            Ok(cfg) => (*cfg).clone(),
            Err(e) => panic!("convert_to failed: {e}"),
        };
        let child_builder = cfg.child_builder.clone();
        let options = LbPolicyOptions { work_scheduler: self.work_scheduler.clone() }; 
        let pending_policy = child_builder.build(options);
        if self.current_policy.lock().unwrap().is_none(){
            *self.current_policy.lock().unwrap() = Some(pending_policy);
            *self.current_policy_builder.lock().unwrap() = Some(child_builder);
            *self.current_policy_state.lock().unwrap() = Some(ConnectivityState::Connecting);
            return ChildKind::Current
        }
        else {
            *self.pending_policy.lock().unwrap() = Some(pending_policy);
            *self.pending_policy_builder.lock().unwrap() = Some(child_builder);
            *self.pending_policy_state.lock().unwrap() = Some(ConnectivityState::Connecting);
            return ChildKind::Pending

            
        }
    }

    fn latest_balancer (&mut self) -> ChildKind{
        if !self.pending_policy.lock().unwrap().is_none(){
            println!("returning pending child kid as latest");
            return ChildKind::Pending
        }
        println!("returning current child kind as latest");
        return ChildKind::Current

    }
}

// Struct to wrap a channel controller around. The purpose is to
// store a picker update to check connectivity state of a child.
// This helps to decide whether to swap or not in subchannel_update.
// Also tracks created_subchannels, which then is then used to map subchannels to 
// children policies.
struct WrappedController<'a> {
    channel_controller: &'a mut dyn ChannelController,
    created_subchannels: Vec<Arc<dyn Subchannel>>,
    picker_update: Option<LbState>,
}

impl<'a> WrappedController<'a> {
    fn new(channel_controller: &'a mut dyn ChannelController) -> Self {
        Self {
            channel_controller,
            created_subchannels: vec![],
            picker_update: None, 
        }
    }
}

impl ChannelController for WrappedController<'_> {
    //call into the real channel controller
    fn new_subchannel(&mut self, address: &Address) -> Arc<dyn Subchannel> {
        println!("new subchannel being called for wrapped controller");
        let subchannel = self.channel_controller.new_subchannel(address);
        self.created_subchannels.push(subchannel.clone());
        subchannel
    }

    fn update_picker(&mut self, update: LbState) {
        let update_clone = update.clone();
        self.channel_controller.update_picker(update);
        self.picker_update = Some(update_clone);
    }

    fn request_resolution(&mut self) {
        self.channel_controller.request_resolution();
    }
}



