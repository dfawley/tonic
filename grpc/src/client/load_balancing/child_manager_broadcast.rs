/*
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    hash::Hash,
    iter::zip,
    mem,
    sync::Arc,
};

use super::{
    ChannelController, LbConfig, LbPolicyBatched as LbPolicy,
    LbPolicyBuilderBatched as LbPolicyBuilder, LbPolicyOptions, LbState, SubchannelUpdate,
    WorkScheduler,
};
use crate::client::name_resolution::{Address, ResolverData, ResolverUpdate};

use super::{Subchannel, SubchannelState};

// An LbPolicy implementation that manages multiple children.
pub struct ChildManager<T> {
    children: Vec<Child<T>>,
    shard_update: Box<ResolverUpdateSharder<T>>,
}

struct Child<T> {
    identifier: T,
    policy: Box<dyn LbPolicy>,
    state: LbState,
}

pub struct ChildUpdate<T> {
    pub child_identifier: T,
    pub child_policy_builder: Box<dyn LbPolicyBuilder>,
    pub child_update: ResolverUpdate,
}

// Shards a ResolverUpdate into ChildUpdates
pub type ResolverUpdateSharder<T> =
    fn(
        ResolverUpdate,
    ) -> Result<Box<dyn Iterator<Item = ChildUpdate<T>>>, Box<dyn Error + Send + Sync>>;

impl<T: PartialEq + Hash + Eq> ChildManager<T> {
    pub fn new(shard_update: Box<ResolverUpdateSharder<T>>) -> Self {
        // Need: access last picker updates, probably just have user call a method to get.
        Self {
            children: Vec::default(),
            shard_update,
        }
    }

    // Returns all children that have produced a state update.
    pub fn child_states(&mut self) -> impl Iterator<Item = (&T, &LbState)> {
        self.children
            .iter()
            .map(|child| (&child.identifier, &child.state))
    }
}

fn resolve_child_controller<T>(channel_controller: WrappedController, child: &mut Child<T>) {
    if let Some(state) = channel_controller.picker_update {
        child.state = state;
    };
}

// ChildManager implements LbPolicy forwarding
impl<T: PartialEq + Hash + Eq + Send> LbPolicy for ChildManager<T> {
    fn resolver_update(
        &mut self,
        resolver_update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // First determine if the incoming update is valid.
        let child_updates = (self.shard_update)(resolver_update)?;

        // Replace self.children with an empty vec.
        let mut old_children = vec![];
        mem::swap(&mut self.children, &mut old_children);

        // Hash the old children for efficient lookups.
        let old_children = old_children
            .into_iter()
            .enumerate()
            .map(|(old_idx, e)| (e.identifier, (e.policy, e.state, old_idx)));
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
            if let Some((policy, state, old_idx)) = old_children.remove(&identifier) {
                self.children.push(Child {
                    identifier,
                    state,
                    policy,
                });
            } else {
                let policy = builder.build(LbPolicyOptions {
                    work_scheduler: Arc::new(NopWorkScheduler {}), /* TODO */
                });
                let state = LbState::initial();
                self.children.push(Child {
                    identifier,
                    state,
                    policy,
                });
            };
        }
        // Anything left in old_children will just be Dropped and cleaned up.

        // Call resolver_update on all children.
        let mut updates = updates.into_iter();
        for child in self.children.iter_mut() {
            let child_update = updates.next().unwrap();
            let mut channel_controller = WrappedController::new(channel_controller);
            let _ = child
                .policy
                .resolver_update(child_update, config, &mut channel_controller);
            resolve_child_controller(channel_controller, child);
        }
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        update: &SubchannelUpdate,
        channel_controller: &mut dyn ChannelController,
    ) {
        for child in self.children.iter_mut() {
            let policy = &mut child.policy;
            let mut channel_controller = WrappedController::new(channel_controller);
            policy.subchannel_update(update, &mut channel_controller);
            resolve_child_controller(channel_controller, child);
        }
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        for child in self.children.iter_mut() {
            let policy = &mut child.policy;
            let mut channel_controller = WrappedController::new(channel_controller);
            policy.work(&mut channel_controller);
            resolve_child_controller(channel_controller, child);
        }
    }
}

pub struct WrappedController<'a> {
    channel_controller: &'a mut dyn ChannelController,
    pub(crate) picker_update: Option<LbState>,
}

impl<'a> WrappedController<'a> {
    fn new(channel_controller: &'a mut dyn ChannelController) -> Self {
        Self {
            channel_controller,
            picker_update: None,
        }
    }
}

impl<'a> ChannelController for WrappedController<'a> {
    fn new_subchannel(&mut self, address: &Address) -> Subchannel {
        self.channel_controller.new_subchannel(address)
    }

    fn update_picker(&mut self, update: LbState) {
        self.picker_update = Some(update);
    }

    fn request_resolution(&mut self) {
        self.channel_controller.request_resolution();
    }
}

pub struct NopWorkScheduler {}
impl WorkScheduler for NopWorkScheduler {
    fn schedule_work(&self) { /* do nothing */
    }
}
*/
