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
    subchannel_child_map: HashMap<Subchannel, usize>,
    // This needs to be Send + Sync, so Arc<Mutex<>> I guess!
    children_requesting_work: HashSet<usize>,
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
            subchannel_child_map: HashMap::default(),
            children_requesting_work: HashSet::default(),
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

    fn resolve_child_controller(
        &mut self,
        channel_controller: WrappedController,
        child_idx: usize,
    ) {
        for csc in channel_controller.created_subchannels {
            self.subchannel_child_map.insert(csc, child_idx);
        }
        if let Some(state) = channel_controller.picker_update {
            self.children[child_idx].state = state;
        };
    }
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

        // Replace the subchannel map with an empty map.
        let mut old_subchannel_child_map = HashMap::new();
        mem::swap(
            &mut self.subchannel_child_map,
            &mut old_subchannel_child_map,
        );
        // Reverse the subchannel map.
        let mut old_child_subchannels_map: HashMap<usize, Vec<Subchannel>> = HashMap::new();
        for (subchannel, child_idx) in old_subchannel_child_map {
            old_child_subchannels_map
                .entry(child_idx)
                .or_insert_with(|| vec![])
                .push(subchannel);
        }

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
                for subchannel in old_child_subchannels_map
                    .remove(&old_idx)
                    .into_iter()
                    .flatten()
                {
                    self.subchannel_child_map.insert(subchannel, new_idx);
                }
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
        for child_idx in 0..self.children.len() {
            let child = &mut self.children[child_idx];
            let child_update = updates.next().unwrap();
            let mut channel_controller = WrappedController::new(channel_controller);
            let _ = child
                .policy
                .resolver_update(child_update, config, &mut channel_controller);
            self.resolve_child_controller(channel_controller, child_idx);
        }
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        update: &SubchannelUpdate,
        channel_controller: &mut dyn ChannelController,
    ) {
        for (subchannel, state) in update.states.iter() {
            let child_idx = *self.subchannel_child_map.get(&subchannel).unwrap();
            let policy = &mut self.children[child_idx].policy;
            let mut channel_controller = WrappedController::new(channel_controller);
            policy.subchannel_update(update, &mut channel_controller);
            self.resolve_child_controller(channel_controller, child_idx);
        }
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        let mut work_requests = HashSet::new();
        mem::swap(&mut self.children_requesting_work, &mut work_requests);

        for child_idx in work_requests {
            let policy = &mut self.children[child_idx].policy;
            let mut channel_controller = WrappedController::new(channel_controller);
            policy.work(&mut channel_controller);
            self.resolve_child_controller(channel_controller, child_idx);
        }
    }
}

pub struct WrappedController<'a> {
    channel_controller: &'a mut dyn ChannelController,
    pub(crate) created_subchannels: Vec<Subchannel>,
    pub(crate) picker_update: Option<LbState>,
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

impl<'a> ChannelController for WrappedController<'a> {
    fn new_subchannel(&mut self, address: &Address) -> Subchannel {
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

pub struct NopWorkScheduler {}
impl WorkScheduler for NopWorkScheduler {
    fn schedule_work(&self) { /* do nothing */
    }
}

 */
