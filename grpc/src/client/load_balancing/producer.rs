use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::client::load_balancing::ChannelController;
use crate::client::load_balancing::LbPolicy;
use crate::client::load_balancing::LbPolicyBuilder;
use crate::client::load_balancing::LbPolicyOptions;
use crate::client::load_balancing::LbState;
use crate::client::load_balancing::PickResult;
use crate::client::load_balancing::Picker;
use crate::client::load_balancing::WorkScheduler;
use crate::client::load_balancing::subchannel::ForwardingSubchannel;
use crate::client::load_balancing::subchannel::Subchannel;
use crate::client::load_balancing::subchannel::SubchannelState;
use crate::client::load_balancing::subchannel::WeakSubchannel;
use crate::client::name_resolution::Address;
use crate::client::name_resolution::ResolverUpdate;
use crate::core::RequestHeaders;

/// A factory for [`Producer`]s.
pub(crate) trait ProducerBuilder: Send + Sync + Debug + 'static {
    type Producer: Producer;

    fn build(
        &self,
        subchannel: &Arc<dyn Subchannel>,
        subchannel_state: &SubchannelState,
        work_scheduler: &Arc<dyn WorkScheduler>,
    ) -> Self::Producer;
}

/// Updates [`SubchannelState`] for the [`Subchannel`] it was registered on;
/// typically by adding an attribute that allows subscribed LB policies to read
/// the data it produces.
///
/// Note that when a [`Producer`] is dropped, it should ensure that it releases
/// any resources it acquired and cancel any tasks it spawned.
pub(crate) trait Producer: Any + Send + Sync + Debug + 'static {
    fn update_state(&self, state: &mut SubchannelState);
}

/// Holds per-subchannel data for producers.
#[derive(Debug)]
struct SubchannelProducerData {
    subchannel: Arc<dyn Subchannel>, // the real subchannel to which this data is related
    producers: Mutex<HashMap<TypeId, Weak<dyn Producer>>>, // the live producers for this subchannel
    pending_update: AtomicBool, // whether any producer requested to update this subchannel's data
    work_scheduler: Arc<dyn WorkScheduler>, // the work scheduler given to new producers
}

/// Implements a [`WorkScheduler`] to be given to [`Producer`]s.  When called,
/// sets a flag in the subchannel data indicating the subchannel should be
/// updated during a call to [`ProducerPolicy`]'s `work` method, and then
/// invokes the channel's [`WorkScheduler`].
#[derive(Debug)]
struct SubchannelProducerWorkScheduler {
    // This work scheduler is stored inside the ProducerSubchannelData and given
    // to the producers on its subchannel, so we store it as a weak reference
    // here to break any possible cycles.  If it can't be promoted then the
    // subchannel storing this data has been shut down, so the producer should
    // also shut down.
    subchannel_data: Weak<SubchannelProducerData>,
}

impl WorkScheduler for SubchannelProducerWorkScheduler {
    fn schedule_work(&self) {
        let Some(subchannel_data) = self.subchannel_data.upgrade() else {
            return;
        };
        subchannel_data
            .pending_update
            .store(true, Ordering::Release);
        subchannel_data.work_scheduler.schedule_work();
    }
}

// Data added to each SubchannelState attributes to allow producers to be
// created by this policy.
#[derive(Clone, Debug)]
struct ProducerPolicyAttribute {
    subchannel_data: Arc<SubchannelProducerData>,
}

/// Builds a new producer for the subchannel of the given type, or returns an
/// Arc to the one that has already been built for the subchannel.  The
/// `producer` module only holds weak references to [`Producer`]s, so when there
/// are no more strong references, the producer will be dropped and should clean
/// up all of its state including any RPCs or other references to the
/// subchannel.
pub(crate) fn get_or_build_producer<T: ProducerBuilder + Any + 'static>(
    producer_builder: T,
    subchannel_state: &SubchannelState,
) -> Arc<T::Producer> {
    let attr = subchannel_state
        .attributes
        .get::<ProducerPolicyAttribute>()
        .expect("subchannel should have producer policy attribute");

    // Return the current producer if it exists and can be upgraded.
    let mut producers = attr.subchannel_data.producers.lock().unwrap();
    if let Some(producer) = producers.get(&producer_builder.type_id())
        && let Some(producer) = producer.upgrade()
    {
        return (producer as Arc<dyn Any + Send + Sync>)
            .downcast::<T::Producer>()
            .unwrap();
    }

    // Otherwise, construct a new producer and add a weak reference to it into
    // the map, potentially displacing the orphaned weak reference.
    let producer = Arc::new(producer_builder.build(
        &attr.subchannel_data.subchannel,
        subchannel_state,
        &attr.subchannel_data.work_scheduler,
    ));

    producers.insert(
        producer_builder.type_id(),
        Arc::downgrade(&producer) as Weak<dyn Producer>,
    );

    // TODO: should this be wrapped in something that can delete the producer
    // from the map???????
    producer
}

impl PartialEq for ProducerPolicyAttribute {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.subchannel_data, &other.subchannel_data)
    }
}

impl Eq for ProducerPolicyAttribute {}

impl PartialOrd for ProducerPolicyAttribute {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProducerPolicyAttribute {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        Arc::as_ptr(&self.subchannel_data).cmp(&Arc::as_ptr(&other.subchannel_data))
    }
}

/// Implements the work scheduler implementation passed to the delegate LB
/// policy.  Tracks whether the delegate asked for work so that when the
/// [`ProducerPolicy`]'s `work` method is called, it knows whether to call
/// `work` on the delegate.
#[derive(Debug)]
struct DelegateWorkScheduler {
    delegate: Arc<dyn WorkScheduler>,
    delegate_work: Arc<AtomicBool>,
}

impl WorkScheduler for DelegateWorkScheduler {
    fn schedule_work(&self) {
        self.delegate_work.store(true, Ordering::Release);
        self.delegate.schedule_work();
    }
}

/// Wraps a Subchannel created from the Channel so that we know to clean up any
/// related state stored for it via weak references.
struct DelegateSubchannel {
    dropped_subchannels: Arc<Mutex<HashSet<Arc<dyn Subchannel>>>>,
    delegate: Arc<dyn Subchannel>,
    work_scheduler: Arc<dyn WorkScheduler>,
}

impl PartialEq for DelegateSubchannel {
    fn eq(&self, other: &Self) -> bool {
        self.delegate.eq(&other.delegate)
    }
}

impl Eq for DelegateSubchannel {}

impl Hash for DelegateSubchannel {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.delegate.hash(state);
    }
}

impl ForwardingSubchannel for DelegateSubchannel {
    fn delegate(&self) -> &Arc<dyn Subchannel> {
        &self.delegate
    }
}

impl Drop for DelegateSubchannel {
    fn drop(&mut self) {
        self.dropped_subchannels
            .lock()
            .unwrap()
            .insert(self.delegate.clone());
        self.work_scheduler.schedule_work();
    }
}

#[derive(Debug)]
struct DelegatePicker {
    delegate: Arc<dyn Picker>,
}

impl Picker for DelegatePicker {
    fn pick(&self, request: &RequestHeaders) -> PickResult {
        let pick = self.delegate.pick(request);
        if let PickResult::Pick(mut pick) = pick {
            let subchannel = pick
                .subchannel
                .downcast_ref::<DelegateSubchannel>()
                .unwrap();
            pick.subchannel = subchannel.delegate.clone();
            return PickResult::Pick(pick);
        }
        pick
    }
}

#[derive(Debug)]
pub(crate) struct ProducerPolicy<T: LbPolicyBuilder> {
    delegate: T::LbPolicy,
    work_scheduler: Arc<dyn WorkScheduler>,
    delegate_work: Arc<AtomicBool>,
    subchannel_datas: HashMap<WeakSubchannel, Arc<SubchannelProducerData>>,
    latest_subchannel_state: HashMap<WeakSubchannel, SubchannelState>,
    dropped_subchannels: Arc<Mutex<HashSet<Arc<dyn Subchannel>>>>,
}

impl<T: LbPolicyBuilder> ProducerPolicy<T> {
    pub(crate) fn new(delegate_builder: T, mut options: LbPolicyOptions) -> Self {
        let delegate_work = Arc::new(AtomicBool::new(false));
        let work_scheduler = options.work_scheduler.clone();
        options.work_scheduler = Arc::new(DelegateWorkScheduler {
            delegate: options.work_scheduler,
            delegate_work: delegate_work.clone(),
        });
        let delegate = delegate_builder.build(options);

        Self {
            delegate,
            work_scheduler,
            delegate_work,
            subchannel_datas: HashMap::default(),
            latest_subchannel_state: HashMap::default(),
            dropped_subchannels: Arc::new(Mutex::default()),
        }
    }

    fn process_subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        channel_controller: &mut dyn ChannelController,
    ) {
        let mut subchannel_state;
        {
            let subchannel_data = self
                .subchannel_datas
                .get(&(&subchannel).into())
                .expect("all subchannels should have data added on creation");
            let mut producers = subchannel_data.producers.lock().unwrap();

            let mut alive_producers = Vec::new();
            producers.retain(|_, weak_producer| {
                if let Some(p) = weak_producer.upgrade() {
                    alive_producers.push(p);
                    true
                } else {
                    false
                }
            });

            subchannel_state = self
                .latest_subchannel_state
                .get(&(&subchannel).into())
                .unwrap()
                .clone();
            for producer in alive_producers {
                producer.update_state(&mut subchannel_state);
            }

            subchannel_state.attributes =
                subchannel_state.attributes.add(ProducerPolicyAttribute {
                    subchannel_data: subchannel_data.clone(),
                });
        }

        let mut wrapped_cc = DelegateChannelController {
            inner: channel_controller,
            subchannels_data: &mut self.subchannel_datas,
            latest_subchannel_state: &mut self.latest_subchannel_state,
            dropped_subchannels: &self.dropped_subchannels,
            work_scheduler: &self.work_scheduler,
        };
        self.delegate
            .subchannel_update(subchannel, &subchannel_state, &mut wrapped_cc);
    }
}

/// Implements [`ChannelController`] when calling the delegate LB policy.
/// Intercepts subchannel creation to add in the [`ProducerPolicy`]'s state and
/// add the subchannel to the maps it stores.  Intercepts picker updates to
/// insert a wrapper that unwraps the picked subchannels.
struct DelegateChannelController<'a> {
    inner: &'a mut dyn ChannelController,
    subchannels_data: &'a mut HashMap<WeakSubchannel, Arc<SubchannelProducerData>>,
    latest_subchannel_state: &'a mut HashMap<WeakSubchannel, SubchannelState>,
    dropped_subchannels: &'a Arc<Mutex<HashSet<Arc<dyn Subchannel>>>>,
    work_scheduler: &'a Arc<dyn WorkScheduler>,
}

impl<'a> ChannelController for DelegateChannelController<'a> {
    fn new_subchannel(&mut self, address: &Address) -> (Arc<dyn Subchannel>, SubchannelState) {
        let (subchannel, mut state) = self.inner.new_subchannel(address);

        let subchannel_data = Arc::new_cyclic(|weak_self| SubchannelProducerData {
            producers: Mutex::default(),
            pending_update: AtomicBool::new(false),
            work_scheduler: Arc::new(SubchannelProducerWorkScheduler {
                subchannel_data: weak_self.clone(),
            }),
            subchannel: subchannel.clone(),
        });
        self.subchannels_data
            .insert(WeakSubchannel::new(&subchannel), subchannel_data.clone());
        self.latest_subchannel_state
            .insert(WeakSubchannel::new(&subchannel), state.clone());
        state.attributes = state
            .attributes
            .add(ProducerPolicyAttribute { subchannel_data });

        let subchannel = Arc::new(DelegateSubchannel {
            dropped_subchannels: self.dropped_subchannels.clone(),
            delegate: subchannel,
            work_scheduler: self.work_scheduler.clone(),
        });

        (subchannel, state)
    }

    fn update_picker(&mut self, mut update: LbState) {
        update.picker = Arc::new(DelegatePicker {
            delegate: update.picker,
        });
        self.inner.update_picker(update)
    }

    fn request_resolution(&mut self) {
        self.inner.request_resolution()
    }
}

impl<T: LbPolicyBuilder> LbPolicy for ProducerPolicy<T> {
    type LbConfig = <T::LbPolicy as LbPolicy>::LbConfig;

    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&Self::LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), String> {
        let mut wrapped_cc = DelegateChannelController {
            inner: channel_controller,
            subchannels_data: &mut self.subchannel_datas,
            dropped_subchannels: &self.dropped_subchannels,
            latest_subchannel_state: &mut self.latest_subchannel_state,
            work_scheduler: &self.work_scheduler,
        };
        self.delegate
            .resolver_update(update, config, &mut wrapped_cc)
    }

    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        let weak_sc = WeakSubchannel::new(&subchannel);

        let subchannel_data = self
            .subchannel_datas
            .get(&weak_sc)
            .expect("all subchannels should have data added on creation");
        *self.latest_subchannel_state.get_mut(&weak_sc).unwrap() = state.clone();

        self.process_subchannel_update(subchannel, channel_controller);
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        // First remove data for any dropped subchannels.
        let mut dropped_subchannels = self.dropped_subchannels.lock().unwrap();
        for subchannel in dropped_subchannels.drain() {
            let weak_sc = (&subchannel).into();
            self.subchannel_datas.remove(&weak_sc);
            self.latest_subchannel_state.remove(&weak_sc);
        }
        drop(dropped_subchannels);

        // Call the delegate if it requested work.
        if self.delegate_work.swap(false, Ordering::AcqRel) {
            let mut wrapped_cc = DelegateChannelController {
                inner: channel_controller,
                subchannels_data: &mut self.subchannel_datas,
                dropped_subchannels: &self.dropped_subchannels,
                latest_subchannel_state: &mut self.latest_subchannel_state,
                work_scheduler: &self.work_scheduler,
            };
            self.delegate.work(&mut wrapped_cc);
        }

        // Determine which producers requested work, if any, and call them,
        // calling subchannel_update afterwards.
        let updates: Vec<Arc<dyn Subchannel>> = self
            .subchannel_datas
            .iter()
            .filter_map(|(sc, h)| {
                if h.pending_update.swap(false, Ordering::AcqRel)
                    && let Some(sc) = sc.upgrade()
                {
                    Some(sc)
                } else {
                    None
                }
            })
            .collect();

        for sc in updates {
            self.process_subchannel_update(sc, channel_controller);
        }
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        let mut wrapped_cc = DelegateChannelController {
            inner: channel_controller,
            subchannels_data: &mut self.subchannel_datas,
            latest_subchannel_state: &mut self.latest_subchannel_state,
            dropped_subchannels: &self.dropped_subchannels,
            work_scheduler: &self.work_scheduler,
        };
        self.delegate.exit_idle(&mut wrapped_cc);
    }
}
