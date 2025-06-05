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
    ChannelController, LbPolicy, LbPolicyBuilder, LbPolicyOptions, LbState, WeakSubchannel,
    WorkScheduler,
};
use crate::client::name_resolution::{Address, ResolverUpdate};
use crate::client::service_config::LbConfig;

use super::{Subchannel, SubchannelState};

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
}

pub trait ChildIdentifier: PartialEq + Hash + Eq + Send + Sync + 'static {}

struct Child {
    policy: Box<dyn LbPolicy>,
    state: LbState,
}

/// A collection of data sent to a child of the ChildManager.
pub struct ChildUpdate {
    /// The builder the ChildManager should use to create this child if it does
    /// not exist.
    pub child_policy_builder: Box<dyn LbPolicyBuilder>,
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
        channel_controller: WrappedController,
        child_id: Arc<T>,
    ) {
        // Add all created subchannels into the subchannel_child_map.
        for csc in channel_controller.created_subchannels {
            self.subchannels
                .insert(WeakSubchannel::new(csc), child_id.clone());
        }
        // Update the tracked state if the child produced an update.
        if let Some(state) = channel_controller.picker_update {
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
            let mut channel_controller = WrappedController::new(channel_controller);
            let _ = child_policy.resolver_update(
                update.child_update.clone(),
                config,
                &mut channel_controller,
            );
            self.resolve_child_controller(channel_controller, child_id.clone());
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
        self.resolve_child_controller(channel_controller, child_id.clone());
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
                self.resolve_child_controller(channel_controller, child_id.clone());
            }
        }
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
        self.picker_update = Some(update);
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
