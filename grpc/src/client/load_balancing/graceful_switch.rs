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

#[cfg(test)]
mod test;

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
struct GracefulSwitchPolicy {
    subchannel_to_policy: HashMap<WeakSubchannel, ChildKind>,
    managing_policy: Mutex<LatestPolicy>,
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

        let mut managing_policy_guard = self.managing_policy.lock().unwrap();

        // Determine which child policy is currently "active" and its name
        let last_policy = managing_policy_guard.latest_balancer_name();

        let mut wrapped_channel_controller = WrappedController::new(channel_controller);
        let update_clone = update.clone();
        let mut target_child_kind = ChildKind::Pending;

        if managing_policy_guard.no_policy() || last_policy != new_builder_name {            
            drop(managing_policy_guard);
            target_child_kind = self.switch_to(config);
            
            managing_policy_guard = self.managing_policy.lock().unwrap();
        }
        match target_child_kind {
            ChildKind::Current => {
                if let Some(ref mut current_policy_instance) = managing_policy_guard.current_child.policy {
                    current_policy_instance.resolver_update(update_clone, cfg.child_config.as_ref(), &mut wrapped_channel_controller)?;
                    if let Some(picker) = wrapped_channel_controller.picker_update.take() {
                        managing_policy_guard.current_child.policy_state = Some(picker.connectivity_state);
                        println!("sending picker of current child");
                        wrapped_channel_controller.channel_controller.update_picker(picker);
                    }
                }
            }

            ChildKind::Pending => {
                println!("Sending resolver_update to Pending Child Policy.");
                if let Some(ref mut pending_policy_instance) = managing_policy_guard.pending_child.policy {
                    pending_policy_instance.resolver_update(update_clone, cfg.child_config.as_ref(), &mut wrapped_channel_controller)?;
                    if let Some(picker) = wrapped_channel_controller.picker_update.take() {
                        managing_policy_guard.pending_child.policy_state = Some(picker.connectivity_state);
                        managing_policy_guard.current_child.policy_picker_update = Some(picker)
                    }
                }
            }
        }
        drop(managing_policy_guard);
        self.resolve_child_controller(&mut wrapped_channel_controller, target_child_kind);
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
            .get(&WeakSubchannel::new(&subchannel.clone()))
            .unwrap_or_else(|| {
                panic!("Subchannel not found in graceful switch: {}", subchannel);
            }).clone(); 
        
        let mut managing_policy = self.managing_policy.lock().unwrap();

        match which_child {
            ChildKind::Pending => {
                if let Some(ref mut pending_policy_instance) = managing_policy.pending_child.policy {
                    pending_policy_instance.subchannel_update(subchannel, state, &mut wrapped_channel_controller);
                    if let Some(picker) = wrapped_channel_controller.picker_update.take() {
                        managing_policy.pending_child.policy_state = Some(picker.connectivity_state);
                        managing_policy.pending_child.policy_picker_update = Some(picker)
                    }
                }
            }

            ChildKind::Current => {
                if let Some(ref mut current_policy_instance) = managing_policy.current_child.policy {
                    current_policy_instance.subchannel_update(subchannel, state, &mut wrapped_channel_controller);
                    if let Some(picker) = wrapped_channel_controller.picker_update.take() {
                        managing_policy.current_child.policy_state = Some(picker.connectivity_state);
                        wrapped_channel_controller.channel_controller.update_picker(picker);
                    }
                }
            }
        }

        // Drop the lock on managing_policy before calling resolve_child_controller
        // as resolve_child_controller also tries to acquire this lock.
        drop(managing_policy);

        // This call will now check the states and potentially swap.
        self.resolve_child_controller(&mut wrapped_channel_controller, which_child);
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        let mut managing_policy = self.managing_policy.lock().unwrap();
        let mut wrapped_channel_controller = WrappedController::new(channel_controller);

        if let Some(ref mut pending_policy_instance) = managing_policy.pending_child.policy {
            pending_policy_instance.work(&mut wrapped_channel_controller);
            drop(managing_policy);
            self.resolve_child_controller(&mut wrapped_channel_controller, ChildKind::Pending);
        } else if let Some(ref mut current_policy_instance) = managing_policy.current_child.policy {
            current_policy_instance.work(&mut wrapped_channel_controller);
            drop(managing_policy);
            self.resolve_child_controller(&mut wrapped_channel_controller, ChildKind::Current);
        } else {
            drop(managing_policy);
        }
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        let mut managing_policy = self.managing_policy.lock().unwrap();
        let mut wrapped_channel_controller = WrappedController::new(channel_controller);
        // Check if there's a pending policy and call its exit_idle method.
        // If not, check the current policy.
        if let Some(ref mut pending_policy_instance) = managing_policy.pending_child.policy {
            pending_policy_instance.exit_idle(&mut wrapped_channel_controller);
            drop(managing_policy);
            self.resolve_child_controller(&mut wrapped_channel_controller, ChildKind::Pending);
        } else if let Some(ref mut current_policy_instance) = managing_policy.current_child.policy {
            current_policy_instance.exit_idle(&mut wrapped_channel_controller);
            drop(managing_policy);
            self.resolve_child_controller(&mut wrapped_channel_controller, ChildKind::Current);
        } else {
            drop(managing_policy);
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
            subchannel_to_policy: HashMap::default(),
            managing_policy: Mutex::new(LatestPolicy::new()),
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
        let mut should_swap = false;
        let balancer_wrapper = self.managing_policy.lock().unwrap();
        match (balancer_wrapper.current_child.policy_state, balancer_wrapper.pending_child.policy_state) {
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
            drop(balancer_wrapper); 
            self.swap(channel_controller);
        }

        for csc in channel_controller.created_subchannels.clone() {
            let key = WeakSubchannel::new(&csc.clone());
            if !self.subchannel_to_policy.contains_key(&key) {
                self.subchannel_to_policy.insert(key, child_kind.clone());
            }
        }
    }

    fn swap(&mut self, channel_controller: &mut WrappedController){
        let mut managing_policy = self.managing_policy.lock().unwrap();
        managing_policy.current_child.policy = managing_policy.pending_child.policy.take();
        managing_policy.current_child.policy_builder = managing_policy.pending_child.policy_builder.take();
        managing_policy.current_child.policy_state = managing_policy.pending_child.policy_state.take();
        self.subchannel_to_policy.retain(|_, v| *v != ChildKind::Current);
        managing_policy.pending_child.policy = None;
        managing_policy.pending_child.policy_builder = None;
        managing_policy.pending_child.policy_state = None;
        if let Some(picker) = managing_policy.pending_child.policy_picker_update.clone(){
            channel_controller.channel_controller.update_picker(picker);
        }
        managing_policy.pending_child.policy_picker_update = None;
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

    fn switch_to (&mut self, 
        config: Option<&LbConfig>) -> ChildKind {
        let cfg: Arc<GracefulSwitchLbConfig> = match config.unwrap().convert_to::<Arc<GracefulSwitchLbConfig>>() {
            Ok(cfg) => (*cfg).clone(),
            Err(e) => panic!("convert_to failed: {e}"),
        };
        let child_builder = cfg.child_builder.clone();
        let options = LbPolicyOptions { work_scheduler: self.work_scheduler.clone() }; 
        let pending_policy = child_builder.build(options);
        let mut balancer_wrapper = self.managing_policy.lock().unwrap();
        if balancer_wrapper.current_child.no_policy(){
            println!("no policy");
            let new_current_child = ChildPolicy::new(Some(child_builder), Some(pending_policy), Some(ConnectivityState::Connecting));
            balancer_wrapper.current_child = new_current_child;
            return ChildKind::Current
        }
        else {
            let new_pending_child = ChildPolicy::new(Some(child_builder), Some(pending_policy), Some(ConnectivityState::Connecting));
            balancer_wrapper.pending_child = new_pending_child;
            return ChildKind::Pending
        }
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
        self.picker_update = Some(update_clone);
    }

    fn request_resolution(&mut self) {
        self.channel_controller.request_resolution();
    }
}


struct ChildPolicy {
    policy_builder: Option<Arc<dyn LbPolicyBuilder>>,
    policy: Option<Box<dyn LbPolicy>>,
    policy_state: Option<ConnectivityState>,
    policy_picker_update: Option<LbState>,
}

impl ChildPolicy {
    fn new(policy_builder: Option<Arc<dyn LbPolicyBuilder>>, policy: Option<Box<dyn LbPolicy>>, policy_state: Option<ConnectivityState>) -> Self {
        ChildPolicy {
            policy_builder,
            policy,
            policy_state,
            policy_picker_update: None
        }
    }
}

struct LatestPolicy {
    current_child: ChildPolicy,
    pending_child: ChildPolicy,
}

impl LatestPolicy {
    fn new() -> Self {
        LatestPolicy {
            current_child: ChildPolicy::new(None, None, None),
            pending_child: ChildPolicy::new(None, None, None),
        }
    }
    
    fn latest_balancer (&mut self) -> (Option<ChildKind>, String) {
        if !self.pending_child.no_policy(){
            return (Some(ChildKind::Pending), self.pending_child.policy_builder.clone().unwrap().name().to_string())
        } else if !self.current_child.no_policy(){
            return (Some(ChildKind::Current), self.current_child.policy_builder.clone().unwrap().name().to_string())
        } else{
            return (None, "".to_string());
        }
    }

    fn latest_balancer_name (&mut self) -> String {
        if !self.pending_child.no_policy(){
            return self.pending_child.policy_builder.clone().unwrap().name().to_string()
        } else if !self.current_child.no_policy(){
            return self.current_child.policy_builder.clone().unwrap().name().to_string()
        } else{
            return "".to_string();
        }
    }


    fn no_policy (&mut self) -> bool {
        if self.pending_child.no_policy() && self.current_child.no_policy(){ 
            return true
        }
        return false
    }
}

impl ChildPolicy {
    fn no_policy (&mut self) -> bool{
        println!("calling no policy");
        return self.policy.is_none()
    }
}