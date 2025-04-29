/*
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    hash::Hash,
    iter::zip,
    mem,
    sync::{Arc, Mutex},
};

use super::{
    ChannelControllerCallbacks as ChannelController, LbConfig,
    LbPolicyBuilderCallbacks as LbPolicyBuilder, LbPolicyCallbacks as LbPolicy, LbPolicyOptions,
    LbPolicySingle, LbState, SubchannelUpdate, SubchannelUpdateFn, WorkScheduler,
};
use crate::client::name_resolution::{Address, ResolverData, ResolverUpdate};

use super::{Subchannel, SubchannelState};

// An LbPolicy implementation that manages multiple children.
// Calls in must all be synchronized via the parent channel's channel_controller.
#[derive(Clone)]
pub struct ChildManager<T: Clone> {
    // inner must be in a mutex even though it is supposed to be only accessed
    // synchronously, because there is no way to guarantee accesses are
    // performed that way, because the callback to update the state cannot pass
    // a &mut self ChildManagerCallbacks as a parameter.
    inner: Arc<Mutex<Inner<T>>>,
}

struct Inner<T> {
    channel_controller: Arc<dyn ChannelController>,
    children: Vec<Child<T>>,
    shard_update: Box<ResolverUpdateSharder<T>>,
    on_child_update: Box<OnChildUpdate<T>>,
}

pub struct Child<T> {
    pub identifier: T,
    policy: Box<dyn LbPolicy>,
    pub state: Arc<Mutex<LbState>>,
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

pub type OnChildUpdate<T> = fn(&Vec<Child<T>>, Arc<dyn ChannelController>);

impl<T: Clone + PartialEq + Hash + Eq> ChildManager<T> {
    pub fn new(
        shard_update: Box<ResolverUpdateSharder<T>>,
        on_child_update: Box<OnChildUpdate<T>>,
        channel_controller: Arc<dyn ChannelController>,
    ) -> Self {
        // Need: access last picker updates, probably just have user call a method to get.
        Self {
            inner: Arc::new(Mutex::new(Inner {
                channel_controller,
                children: Vec::default(),
                shard_update,
                on_child_update,
            })),
        }
    }
}

// ChildManager implements LbPolicy forwarding
impl<T: Clone + PartialEq + Hash + Eq + Send + 'static> LbPolicy for ChildManager<T> {
    fn resolver_update(
        &mut self,
        resolver_update: ResolverUpdate,
        config: Option<&LbConfig>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut inner = self.inner.lock().unwrap();
        // First determine if the incoming update is valid.
        let child_updates = (inner.shard_update)(resolver_update)?;

        // Replace self.children with an empty vec.
        let mut old_children = vec![];
        mem::swap(&mut inner.children, &mut old_children);

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
                inner.children.push(Child {
                    identifier,
                    state,
                    policy,
                });
            } else {
                let state = Arc::new(Mutex::new(LbState::initial()));
                let channel_controller = Arc::new(WrappedControllerCallbacks::new(
                    inner.channel_controller.clone(),
                    state.clone(),
                    self.inner.clone(),
                ));
                let policy = builder.build(
                    LbPolicyOptions {
                        work_scheduler: Arc::new(NopWorkScheduler {}), /* TODO */
                    },
                    channel_controller,
                );
                inner.children.push(Child {
                    identifier,
                    state,
                    policy,
                });
            };
        }
        // Anything left in old_children will just be Dropped and cleaned up.

        // Call resolver_update on all children.
        let mut updates = updates.into_iter();
        for child_idx in 0..inner.children.len() {
            let policy = &mut inner.children[child_idx].policy;
            let child_update = updates.next().unwrap();
            let _ = policy.resolver_update(child_update, config);
        }
        Ok(())
    }
}

impl<T> Inner<T> {
    fn on_update(&self) {
        (self.on_child_update)(&self.children, self.channel_controller.clone());
    }
}

pub struct WrappedControllerCallbacks<T> {
    channel_controller: Arc<dyn ChannelController>,
    state: Arc<Mutex<LbState>>,
    parent: Arc<Mutex<Inner<T>>>,
}

impl<T> WrappedControllerCallbacks<T> {
    fn new(
        channel_controller: Arc<dyn ChannelController>,
        state: Arc<Mutex<LbState>>,
        parent: Arc<Mutex<Inner<T>>>,
    ) -> Self {
        Self {
            channel_controller,
            state,
            parent,
        }
    }
}

impl<T: Send> ChannelController for WrappedControllerCallbacks<T> {
    fn new_subchannel(&self, address: &Address, updates: SubchannelUpdateFn) -> Subchannel {
        self.channel_controller.new_subchannel(address, updates)
    }

    fn update_picker(&self, update: LbState) {
        *self.state.lock().unwrap() = update;
        self.parent.lock().unwrap().on_update();
    }

    fn request_resolution(&self) {
        self.channel_controller.request_resolution();
    }
}

pub struct NopWorkScheduler {}
impl WorkScheduler for NopWorkScheduler {
    fn schedule_work(&self) { /* do nothing */
    }
}

*/
