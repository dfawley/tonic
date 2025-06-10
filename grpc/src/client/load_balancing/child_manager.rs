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

//! A utility which helps parent LB policies manage multiple children for the
//! purposes of forwarding channel updates.

// TODO: This is mainly provided as a fairly complex example of the current LB
// policy in use.  Complete tests must be written before it can be used in
// production.  Also, support for the work scheduler is missing.

use std::collections::HashSet;
use std::sync::Mutex;
use std::{collections::HashMap, error::Error, hash::Hash, mem, sync::Arc};

use crate::client::load_balancing::{
    ChannelController, ExternalSubchannel, Failing, LbConfig, LbPolicy, LbPolicyBuilder,
    LbPolicyOptions, LbState, ParsedJsonLbConfig, PickResult, Picker, QueuingPicker,
    Subchannel, SubchannelState, WeakSubchannel, WorkScheduler, GLOBAL_LB_REGISTRY, ConnectivityState,
};
use crate::client::name_resolution::{Address, ResolverUpdate};
use crate::service::{Message, Request, Response, Service};

use tonic::{metadata::MetadataMap, Status};

use tokio::sync::{mpsc, watch, Notify};
use tokio::task::{AbortHandle, JoinHandle};

// An LbPolicy implementation that manages multiple children.
pub struct ChildManager<T> {
    subchannels: HashMap<WeakSubchannel, Arc<T>>,
    children: HashMap<Arc<T>, Child>,
    sharder: Box<dyn ResolverUpdateSharder<T>>,
    updated: bool, // true iff a child has updated its state since the last call to has_updated.
    work_requests: Arc<Mutex<HashSet<Arc<T>>>>,
    work_scheduler: Arc<dyn WorkScheduler>,
    sent_connecting_state: bool,
    aggregated_state: ConnectivityState,
    
}

use std::{sync::{atomic::{AtomicUsize, Ordering}}};

pub trait ChildIdentifier: PartialEq + Hash + Eq + Send + Sync + 'static {}

struct Child {
    policy: Box<dyn LbPolicy>,
    state: LbState,
}

/// A collection of data sent to a child of the ChildManager.
pub struct ChildUpdate {
    /// The builder the ChildManager should use to create this child if it does
    /// not exist.
    pub child_policy_builder: Arc<dyn LbPolicyBuilder>,
    /// The relevant ResolverUpdate to send to this child.
    pub child_update: ResolverUpdate,
}

pub trait ResolverUpdateSharder<T: ChildIdentifier>: Send {
    /// Performs the operation of sharding an aggregate ResolverUpdate into one
    /// or more ChildUpdates.  Called automatically by the ChildManager when its
    /// resolver_update method is called.  The key in the returned map is the
    /// identifier the ChildManager should use for this child.
    fn shard_update(
        &self,
        resolver_update: ResolverUpdate,
    ) -> Result<HashMap<T, ChildUpdate>, Box<dyn Error + Send + Sync>>;
}

impl<T: ChildIdentifier> ChildManager<T> {
    /// Creates a new ChildManager LB policy.  shard_update is called whenever a
    /// resolver_update operation occurs.
    pub fn new(
        work_scheduler: Arc<dyn WorkScheduler>,
        sharder: Box<dyn ResolverUpdateSharder<T>>,
    ) -> Self {
        ChildManager {
            subchannels: HashMap::default(),
            children: HashMap::default(),
            sharder,
            updated: false,
            work_requests: Arc::default(),
            work_scheduler,
            sent_connecting_state: false,
            aggregated_state: ConnectivityState::Idle,
        }
    }

    /// Returns data for all current children.
    pub fn child_states(&mut self) -> impl Iterator<Item = (&T, &LbState)> {
        self.children
            .iter()
            .map(|(id, child)| (id.as_ref(), &child.state))
    }

    pub fn has_updated(&mut self) -> bool {
        mem::take(&mut self.updated)
    }



    // Called to update all accounting in the ChildManager from operations
    // performed by a child policy on the WrappedController that was created for
    // it.
    //
    // TODO: this post-processing step can be eliminated by capturing the right
    // state inside the WrappedController, however it is fairly complex.  Decide
    // which way is better.
    fn resolve_child_controller(
        &mut self,
        is_updating_subchannel: bool,
        channel_controller: &mut WrappedController,
        
        child_id: Arc<T>,
    ) {
        // Add all created subchannels into the subchannel_child_map.
        for csc in channel_controller.created_subchannels.clone() {
            self.subchannels
                .insert(WeakSubchannel::new(csc), child_id.clone());
        }
        // Update the tracked state if the child produced an update.
        if let Some(state) = channel_controller.picker_update.clone() {
            self.children.get_mut(&child_id.clone()).unwrap().state = state;
            self.updated = true;
        };
        // Prune subchannels created by this child that are no longer
        // referenced.
        self.subchannels.retain(|sc, cid| {
            if cid != &child_id {
                return true;
            }
            if sc.upgrade().is_none() {
                return false;
            }
            true
        });
        self.aggregate_states(channel_controller);

    }

    fn aggregate_states (& mut self, channel_controller: &mut WrappedController,) {
        
        println!("Current connectivity state: {}", self.aggregated_state);
        let current_connectivity_state = self.aggregated_state.clone();
        let child_states_vec: Vec<_> = self.child_states().collect();


        let mut transient_failure_picker = TransientFailurePickers::new("error string I guess".to_string());

        let mut connecting_pickers = ConnectingPickers::new();

        let mut ready_pickers = ReadyPickers::new();

        let mut has_idle = false;
        // if current_connectivity_state == ConnectivityState::TransientFailure
        //     && !ready_pickers.has_any()
        // {
        //     // Stay in TF, do not send a Connecting picker.
        //     return;
        // }
        for (child, state) in &child_states_vec {
            match state.connectivity_state {
                ConnectivityState::Idle => {
                    connecting_pickers.add_picker(state.picker.clone());
                    has_idle = true;
                    // connecting_pickers.add_picker(state.picker.clone());
                }
                ConnectivityState::Connecting  => {
                    connecting_pickers.add_picker(state.picker.clone());
                }
                ConnectivityState::Ready => {
                    ready_pickers.add_picker(state.picker.clone());
                }
                ConnectivityState::TransientFailure =>{
                    transient_failure_picker.add_picker(state.picker.clone());
                }
                
               
            }
        }


        let has_ready = ready_pickers.has_any();
        let has_connecting = connecting_pickers.pickers.len() >= 1;
        // let has_idle = idle_picker.pickers.len() >= 1;
        let has_transient_failure = transient_failure_picker.pickers.len() >= 1;
        let all_transient_failure = has_transient_failure
            && !has_ready
            && !has_connecting;
            // && !has_idle;

        // Decide the new aggregate state
        let new_state = if has_ready {
            ConnectivityState::Ready
        }  else if has_connecting {
            ConnectivityState::Connecting
        } else  {
            ConnectivityState::TransientFailure
        };
        self.aggregated_state = new_state;
        println!("new state is {}", new_state);

        // Latch: If we're in TF and new state is not Ready, stay in TF and do nothing
        

        // Now update state and send picker as appropriate
        match new_state {
            ConnectivityState::Ready => {
                if current_connectivity_state != ConnectivityState::Ready{
                    println!("sending ready picker update");
                    self.aggregated_state = ConnectivityState::Ready;
                    let picker = Arc::new(ready_pickers);
                    channel_controller.update_picker(LbState {
                        connectivity_state: ConnectivityState::Ready,
                        picker,
                    });
                    self.sent_connecting_state = false;
                }
            }
            ConnectivityState::Connecting => {
                if self.aggregated_state == ConnectivityState::TransientFailure
                    && new_state != ConnectivityState::Ready
                {
                    return;
                }
                if !self.sent_connecting_state {
                    println!("sending connecting picker update");
                    self.aggregated_state = ConnectivityState::Connecting;
                    let picker = Arc::new(QueuingPicker {});
                    self.move_to_connecting(has_idle, channel_controller, picker);
                }
                // Don't send another Connecting picker if already sent
            }
            ConnectivityState::Idle => {
                if !self.sent_connecting_state {
                    println!("sending connecting picker update");
                    self.aggregated_state = ConnectivityState::Connecting;
                    let picker = Arc::new(QueuingPicker {});
                    self.move_to_connecting(has_idle, channel_controller, picker);
                }
                // Don't send another Connecting picker if already sent
            }
            ConnectivityState::TransientFailure => {
                if current_connectivity_state != ConnectivityState::TransientFailure{
                    self.aggregated_state = ConnectivityState::TransientFailure;
                    let picker = Arc::new(transient_failure_picker);
                    self.move_to_transient_failure(channel_controller, picker);
                }
            }
   
        }

    }
}

impl<T: ChildIdentifier> ChildManager<T> {
    fn move_to_transient_failure(&mut self, channel_controller: &mut WrappedController, picker: Arc<dyn Picker>) {
        channel_controller.update_picker(LbState {
                connectivity_state: ConnectivityState::TransientFailure,
               picker: picker,
            });
        println!("requesting resolution");
        // channel_controller.request_resolution();
        self.sent_connecting_state = false;
    }



    fn move_to_connecting(&mut self, is_idle: bool, channel_controller: &mut WrappedController, picker: Arc<dyn Picker>) {
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::Connecting,
            picker: picker,
        });
        self.sent_connecting_state = true;
        if is_idle {
            println!("requesting resolution");
            channel_controller.request_resolution();
        }
    }
}

impl<T: ChildIdentifier> LbPolicy for ChildManager<T> {
    fn resolver_update(
        &mut self,
        resolver_update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // First determine if the incoming update is valid.
        let mut channel_controller = WrappedController::new(channel_controller);
        if let Err(error) = &resolver_update.endpoints {
        // Move to TransientFailure with the resolver error
            let picker = Arc::new(TransientFailurePickers::new(error.to_string()));
            
            let err = "error".to_string();
            self.move_to_transient_failure(&mut channel_controller, picker);
            return Ok(());
        }
        let child_updates = self.sharder.shard_update(resolver_update)?;

        // Remove children that are no longer active.
        self.children
            .retain(|child_id, _| child_updates.contains_key(child_id));

        // Apply child updates to respective policies, instantiating new ones as
        // needed.
        for (id, update) in child_updates.into_iter() {
            let child_id: Arc<T> = Arc::new(id);
            let child_policy: &mut dyn LbPolicy = match self.children.get_mut(&child_id) {
                Some(child) => child.policy.as_mut(),
                None => {
                    self.children.insert(
                        child_id.clone(),
                        Child {
                            policy: update.child_policy_builder.build(LbPolicyOptions {
                                work_scheduler: Arc::new(ChildScheduler::new(
                                    child_id.clone(),
                                    self.work_scheduler.clone(),
                                    self.work_requests.clone(),
                                )),
                            }),
                            state: LbState::initial(),
                        },
                    );
                    self.children.get_mut(&child_id).unwrap().policy.as_mut()
                }
            };
            
            let _ = child_policy.resolver_update(
                update.child_update.clone(),
                config,
                &mut channel_controller,
            );
            self.resolve_child_controller(false, &mut channel_controller, child_id.clone());
        }

        // Keep only the subchannels associated with currently active children.
        self.subchannels
            .retain(|_, child_id| self.children.contains_key(child_id));
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        // Determine which child created this subchannel.
        let child_id = self
            .subchannels
            .get(&WeakSubchannel::new(subchannel.clone()))
            .unwrap_or_else(|| {
                panic!("Subchannel not found in child manager: {}", subchannel);
            });
        let policy = &mut self.children.get_mut(&child_id.clone()).unwrap().policy;

        // Wrap the channel_controller to track the child's operations.
        let mut channel_controller = WrappedController::new(channel_controller);
        // Call the proper child.
        policy.subchannel_update(subchannel, state, &mut channel_controller);
        self.resolve_child_controller(true, &mut channel_controller, child_id.clone());
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        let children = mem::take(&mut *self.work_requests.lock().unwrap());
        // It is possible that work was queued for a child that got removed as
        // part of a subsequent resolver_update. So, it is safe to ignore such a
        // child here.
        for child_id in children {
            if let Some(child) = self.children.get_mut(&child_id) {
                let mut channel_controller = WrappedController::new(channel_controller);
                child.policy.work(&mut channel_controller);
                self.resolve_child_controller(false, &mut channel_controller, child_id.clone());
            }
        }
    }
    
    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        todo!()
        // let policy = &mut self.children.get_mut(&child_id.clone()).unwrap().policy;
        // let mut channel_controller = WrappedController::new(channel_controller);
        // // Call the proper child.
        // policy.exit_idle(&mut channel_controller);
        // self.resolve_child_controller(channel_controller, child_id.clone());
    }
}

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
    fn new_subchannel(&mut self, address: &Address) -> Arc<dyn Subchannel> {
        let subchannel = self.channel_controller.new_subchannel(address);
        self.created_subchannels.push(subchannel.clone());
        subchannel
    }

    fn update_picker(&mut self, update: LbState) {
            self.picker_update = Some(update.clone());
            self.channel_controller.update_picker(update);
        }

    fn request_resolution(&mut self) {
        self.channel_controller.request_resolution();
    }
}

struct ChildScheduler<T: ChildIdentifier> {
    child_identifier: Arc<T>,
    work_requests: Arc<Mutex<HashSet<Arc<T>>>>,
    work_scheduler: Arc<dyn WorkScheduler>,
}

impl<T: ChildIdentifier> ChildScheduler<T> {
    fn new(
        child_identifier: Arc<T>,
        work_scheduler: Arc<dyn WorkScheduler>,
        work_requests: Arc<Mutex<HashSet<Arc<T>>>>,
    ) -> Self {
        Self {
            child_identifier,
            work_requests,
            work_scheduler,
        }
    }
}

impl<T: ChildIdentifier> WorkScheduler for ChildScheduler<T> {
    fn schedule_work(&self) {
        (*self.work_requests.lock().unwrap()).insert(self.child_identifier.clone());
        self.work_scheduler.schedule_work();
    }
}

struct ReadyPickers {
    pickers: Vec<Arc<dyn Picker>>,
    next: AtomicUsize,
}



impl ReadyPickers {
    pub fn new() -> Self {
        Self {
            pickers: vec![],
            next: AtomicUsize::new(0),
        }
    }

    fn add_picker(&mut self, picker: Arc<dyn Picker>)  {
        self.pickers.push(picker);
    }

    pub fn has_any(&self) -> bool {
        !self.pickers.is_empty()
    }
}

impl Picker for ReadyPickers {
    fn pick(&self, request: &Request) -> PickResult {
        let len = self.pickers.len();
        if len == 0 {
            return PickResult::Queue;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % len;
        self.pickers[idx].pick(request)
    }

    
}
struct ConnectingPickers {
    pickers: Vec<Arc<dyn Picker>>,
    // next: AtomicUsize,
}

impl Picker for ConnectingPickers {
    fn pick(&self, request: &Request) -> PickResult {
        
        return PickResult::Queue;
        
    }
}

impl ConnectingPickers {
    pub fn new() -> Self {
        Self {
            pickers: vec![],
        }
    }

    fn add_picker(&mut self, picker: Arc<dyn Picker>)  {
        self.pickers.push(picker);
    }
}

struct TransientFailurePickers {
    pickers: Vec<Arc<dyn Picker>>,
    error: String,    

}

impl Picker for TransientFailurePickers {
    fn pick(&self, request: &Request) -> PickResult {
        
        return PickResult::Fail(Status::unavailable(self.error.clone()))
        
    }
}

impl TransientFailurePickers {
    pub fn new(error: String) -> Self {
        Self {
            pickers: vec![],
            error,
        }
    }

    fn add_picker(&mut self, picker: Arc<dyn Picker>)  {
        self.pickers.push(picker);
    }
}
struct IdlePickers {
    pickers: Vec<Arc<dyn Picker>>,
    // next: AtomicUsize,
}

impl Picker for IdlePickers {
    fn pick(&self, request: &Request) -> PickResult {
        
        return PickResult::Queue;
        
    }
}

impl IdlePickers {
    pub fn new() -> Self {
        Self {
            pickers: vec![],
        }
    }

    fn add_picker(&mut self, picker: Arc<dyn Picker>)  {
        self.pickers.push(picker);
    }
}