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
use std::fmt::Display;
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
    subchannel_child_map: HashMap<WeakSubchannel, usize>,
    children: Vec<Child<T>>,
    update_sharder: Box<dyn ResolverUpdateSharder<T>>,
    pending_work: Arc<Mutex<HashSet<usize>>>,
    updated: bool, // true iff a child has updated its state since the last call to has_updated.
    // work_requests: Arc<Mutex<HashSet<Arc<T>>>>,
    // work_scheduler: Arc<dyn WorkScheduler>,
    sent_connecting_state: bool,
    aggregated_state: ConnectivityState,
    last_ready_pickers: Vec<Arc<dyn Picker>>,
    
}

use std::{sync::{atomic::{AtomicUsize, Ordering}}};

pub trait ChildIdentifier: PartialEq + Hash + Eq + Send + Sync + Display + 'static {}


struct Child<T> {
    identifier: T,
    policy: Box<dyn LbPolicy>,
    state: LbState,
    work_scheduler: Arc<ChildWorkScheduler>,
}

/// A collection of data sent to a child of the ChildManager.
pub struct ChildUpdate<T> {
    /// The identifier the ChildManager should use for this child.
    pub child_identifier: T,
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
    ) -> Result<Box<dyn Iterator<Item = ChildUpdate<T>>>, Box<dyn Error + Send + Sync>>;
}

impl<T: ChildIdentifier> ChildManager<T> {
    /// Creates a new ChildManager LB policy.  shard_update is called whenever a
    /// resolver_update operation occurs.
    pub fn new(
        update_sharder: Box<dyn ResolverUpdateSharder<T>>
    ) -> Self {
        ChildManager {
            update_sharder,
            subchannel_child_map: Default::default(),
            children: Default::default(),
            pending_work: Default::default(),
            updated: false,
            sent_connecting_state: false,
            aggregated_state: ConnectivityState::Idle,
            last_ready_pickers: Vec::new(),
        }
    }

    /// Returns data for all current children.
    pub fn child_states(&mut self) -> impl Iterator<Item = (&T, &LbState)> {
        self.children
            .iter()
            .map(|child| (&child.identifier, &child.state))
    }
    
    pub fn has_updated(&mut self) -> bool {
        mem::take(&mut self.updated)
    }



    // Called to update all accounting in the ChildManager from operations
    // performed by a child policy on the WrappedController that was created for
    // it.  child_idx is an index into the children map for the relevant child.
    //
    // TODO: this post-processing step can be eliminated by capturing the right
    // state inside the WrappedController, however it is fairly complex.  Decide
    // which way is better.
    fn resolve_child_controller(
        &mut self,
        channel_controller: &mut WrappedController,        
        child_idx: usize,
    ) {
        // Add all created subchannels into the subchannel_child_map.
        for csc in channel_controller.created_subchannels.clone() {
            self.subchannel_child_map.insert(csc.into(), child_idx);
        }
        // Update the tracked state if the child produced an update.
        if let Some(state) = &channel_controller.picker_update {
            self.children[child_idx].state = state.clone();
            self.updated = true;
        };

  
        if self.has_updated() {
            self.aggregate_states(channel_controller);

        }
        

    }
    // Called to aggregate states from pick first children. Sends a picker to
    // the channel based on aggregation
    fn aggregate_states (& mut self, channel_controller: &mut WrappedController) {
        let current_connectivity_state = self.aggregated_state.clone();
        let child_states_vec: Vec<_> = self.child_states().collect();
        // Constructing pickers to return
        let mut transient_failure_picker = TransientFailurePickers::new("error string I guess".to_string());
        let mut connecting_pickers = ConnectingPickers::new();
        let mut ready_pickers = ReadyPickers::new();

        let mut has_idle = false;
        let mut is_transient_failure = true;

        for (child_id, state) in &child_states_vec {
            match state.connectivity_state {
                ConnectivityState::Idle => {
                    connecting_pickers.add_picker(state.picker.clone());
                    has_idle = true;
                    is_transient_failure = false;
                }
                ConnectivityState::Connecting  => {
                    connecting_pickers.add_picker(state.picker.clone());
                    is_transient_failure = false;
                }
                ConnectivityState::Ready => {
                    ready_pickers.add_picker(state.picker.clone());
                    is_transient_failure = false;
                }
                ConnectivityState::TransientFailure =>{
                    transient_failure_picker.add_picker(state.picker.clone());
                }
            }
        }

        let has_ready = ready_pickers.has_any();
        let has_connecting = connecting_pickers.pickers.len() >= 1;
        

        // Decide the new aggregate state
        let new_state = if has_ready {
            ConnectivityState::Ready
        }  else if has_connecting {
            ConnectivityState::Connecting
        } else if is_transient_failure{
            ConnectivityState::TransientFailure
        } else{
            ConnectivityState::Connecting
        };

        self.aggregated_state = new_state;

       

        // Now update state and send picker as appropriate
        match new_state {
            ConnectivityState::Ready => {
                let pickers_vec = ready_pickers.pickers.clone();
                let picker: Arc<dyn Picker> = Arc::new(ready_pickers);
                let should_update = !self.compare_prev_to_new_pickers(&self.last_ready_pickers, &pickers_vec);

              
                if should_update || self.aggregated_state != ConnectivityState::Ready {
                    println!("child manager sends ready picker update");
                    self.aggregated_state = ConnectivityState::Ready;
                    self.last_ready_pickers = pickers_vec;
                    channel_controller.need_to_reach();
                    channel_controller.update_picker(LbState {
                        connectivity_state: ConnectivityState::Ready,
                        picker,
                    });
                    println!("connecting state should be false");
                    self.sent_connecting_state = false;
                }
            }
            ConnectivityState::Connecting => {
                if self.aggregated_state == ConnectivityState::TransientFailure
                    && new_state != ConnectivityState::Ready
                {
                    println!("connecting state should be false");
                    return;
                }
                if !self.sent_connecting_state {
                    println!("child manager sends connecting picker update");
                    
                    channel_controller.need_to_reach();
                    let picker = Arc::new(QueuingPicker{});
                    self.move_to_connecting(has_idle, channel_controller, picker);
                } else{
                    println!("child manager is not sending connecting picker as it has alrady sent connecting state");
                }
                // Don't send another Connecting picker if already sent
            }
            ConnectivityState::Idle => {
                self.aggregated_state = ConnectivityState::Connecting;
                let picker = Arc::new(QueuingPicker {});
                channel_controller.need_to_reach();
                channel_controller.update_picker(LbState {
                    connectivity_state: ConnectivityState::Connecting,
                    picker,
                });
                
                self.sent_connecting_state = true;
            }
            ConnectivityState::TransientFailure => {
                if current_connectivity_state != ConnectivityState::TransientFailure{
                    println!("child manager sends transient failure picker update");
                    let picker = Arc::new(transient_failure_picker);
                    channel_controller.need_to_reach();
                    self.move_to_transient_failure(channel_controller, picker);
                }
            }
   
        }
    }
    
    
}

impl<T: ChildIdentifier> ChildManager<T> {
    fn move_to_transient_failure(&mut self, channel_controller: &mut dyn ChannelController, picker: Arc<dyn Picker>) {
        self.aggregated_state = ConnectivityState::TransientFailure;
        channel_controller.update_picker(LbState {
                connectivity_state: ConnectivityState::TransientFailure,
               picker: picker,
            });
        println!("requesting resolution");
        channel_controller.request_resolution();
        self.sent_connecting_state = false;
    }



    fn move_to_connecting(&mut self, is_idle: bool, channel_controller: &mut dyn ChannelController, picker: Arc<dyn Picker>) {
        self.aggregated_state = ConnectivityState::Connecting;
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::Connecting,
            picker: picker,
        });
        self.sent_connecting_state = true;
        // if is_idle {
        //     println!("requesting resolution");
        //     channel_controller.request_resolution();
        // }
    }

    fn compare_prev_to_new_pickers(& self, old_pickers: &[Arc<dyn Picker>], new_pickers: &[Arc<dyn Picker>]) -> bool {
        //if length is different, then definitely not the same picker
        if old_pickers.len() != new_pickers.len() {
            return false;
        }
        //compares two vectors of pickers by pointer equality and returns true if all pickers are the same 
        for (x, y) in old_pickers.iter().zip(new_pickers.iter()) {
            if !Arc::ptr_eq(x, y) {
                return false;
            }
        }
        true
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
        let child_updates = self.update_sharder.shard_update(resolver_update)?;

        // Hold the lock to prevent new work requests during this operation and
        // rewrite the indices.
        let mut pending_work = self.pending_work.lock().unwrap();

        // Reset pending work; we will re-add any entries it contains with the
        // right index later.
        let old_pending_work = mem::take(&mut *pending_work);

        // Replace self.children with an empty vec.
        let old_children = mem::take(&mut self.children);

        // Replace the subchannel map with an empty map.
        let old_subchannel_child_map = mem::take(&mut self.subchannel_child_map);

        // Reverse the old subchannel map.
        let mut old_child_subchannels_map: HashMap<usize, Vec<WeakSubchannel>> = HashMap::new();

        for (subchannel, child_idx) in old_subchannel_child_map {
            old_child_subchannels_map
                .entry(child_idx)
                .or_default()
                .push(subchannel);
        }

        // Build a map of the old children from their IDs for efficient lookups.
        let old_children = old_children
            .into_iter()
            .enumerate()
            .map(|(old_idx, e)| (e.identifier, (e.policy, e.state, old_idx, e.work_scheduler)));
        let mut old_children: HashMap<T, _> = old_children.collect();

        // Split the child updates into the IDs and builders, and the
        // ResolverUpdates.
        let (ids_builders, updates): (Vec<_>, Vec<_>) = child_updates
            .map(|e| ((e.child_identifier, e.child_policy_builder), e.child_update))
            .unzip();

        // Transfer children whose identifiers appear before and after the
        // update, and create new children.  Add entries back into the
        // subchannel map.
        for (new_idx, (identifier, builder)) in ids_builders.into_iter().enumerate() {
            if let Some((policy, state, old_idx, work_scheduler)) = old_children.remove(&identifier)
            {
                for subchannel in old_child_subchannels_map
                    .remove(&old_idx)
                    .into_iter()
                    .flatten()
                {
                    self.subchannel_child_map.insert(subchannel, new_idx);
                }
                if old_pending_work.contains(&old_idx) {
                    pending_work.insert(new_idx);
                }
                *work_scheduler.idx.lock().unwrap() = Some(new_idx);
                self.children.push(Child {
                    identifier,
                    state,
                    policy,
                    work_scheduler,
                });
            } else {
                let work_scheduler = Arc::new(ChildWorkScheduler {
                    pending_work: self.pending_work.clone(),
                    idx: Mutex::new(Some(new_idx)),
                });
                let policy = builder.build(LbPolicyOptions {
                    work_scheduler: work_scheduler.clone(),
                });
                let state = LbState::initial();
                self.children.push(Child {
                    identifier,
                    state,
                    policy,
                    work_scheduler,
                });
            };
        }

        // Invalidate all deleted children's work_schedulers.
        for (_, (_, _, _, work_scheduler)) in old_children {
            *work_scheduler.idx.lock().unwrap() = None;
        }

        // Release the pending_work mutex before calling into the children to
        // allow their work scheduler calls to unblock.
        drop(pending_work);

        // Anything left in old_children will just be Dropped and cleaned up.

        // Call resolver_update on all children.
        let mut updates = updates.into_iter();
        for child_idx in 0..self.children.len() {
            let child = &mut self.children[child_idx];
            let child_update = updates.next().unwrap();
            let mut channel_controller = WrappedController::new(channel_controller);
            let _ = child
                .policy
                .resolver_update(child_update, config, &mut channel_controller);
            self.resolve_child_controller(&mut channel_controller, child_idx);
        }
        Ok(())
    }
    

    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        // Determine which child created this subchannel.
        let child_idx = *self
            .subchannel_child_map
            .get(&WeakSubchannel::new(&subchannel))
            .unwrap();
        let policy = &mut self.children[child_idx].policy;
        // Wrap the channel_controller to track the child's operations.
        let mut channel_controller = WrappedController::new(channel_controller);
        // Call the proper child.
        policy.subchannel_update(subchannel, state, &mut channel_controller);
        self.resolve_child_controller(&mut channel_controller, child_idx);
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        let child_idxes = mem::take(&mut *self.pending_work.lock().unwrap());
        for child_idx in child_idxes {
            let mut channel_controller = WrappedController::new(channel_controller);
            self.children[child_idx]
                .policy
                .work(&mut channel_controller);
            self.resolve_child_controller(&mut channel_controller, child_idx);
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
    need_to_reach: bool,
}

impl<'a> WrappedController<'a> {
    fn new(channel_controller: &'a mut dyn ChannelController) -> Self {
        Self {
            channel_controller,
            created_subchannels: vec![],
            picker_update: None,
            need_to_reach: false,
        }
    }

    fn need_to_reach(&mut self) {
        self.need_to_reach = true;
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
            if self.need_to_reach{
                self.channel_controller.update_picker(update);
                self.need_to_reach = false;
            }
        }

    fn request_resolution(&mut self) {
        self.channel_controller.request_resolution();
    }
}

struct ChildWorkScheduler {
    pending_work: Arc<Mutex<HashSet<usize>>>, // Must be taken first for correctness
    idx: Mutex<Option<usize>>,                // None if the child is deleted.
}

impl WorkScheduler for ChildWorkScheduler {
    fn schedule_work(&self) {
        let mut pending_work = self.pending_work.lock().unwrap();
        if let Some(idx) = *self.idx.lock().unwrap() {
            pending_work.insert(idx);
        }
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
    work_scheduler: Arc<dyn WorkScheduler>,
    // next: AtomicUsize,
}

struct SchedulingIdlePicker {
    work_scheduler: Arc<dyn WorkScheduler>,
}

impl Picker for SchedulingIdlePicker {
    fn pick(&self, _request: &Request) -> PickResult {
        self.work_scheduler.schedule_work();
        PickResult::Queue
    }
}

