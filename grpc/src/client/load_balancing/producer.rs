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

    /// Creates a producer for the subchannel provided.  Includes the current
    /// state of the subchannel for informational purposes and a work_scheduler
    /// that can be used when the producer needs to provide a subchannel state
    /// update to that producer's subscribers.
    fn build(
        &self,
        subchannel: &Arc<dyn Subchannel>,
        subchannel_state: &SubchannelState,
        work_scheduler: &Arc<dyn WorkScheduler>,
    ) -> Self::Producer;
}

/// A plugin capable of altering the state of a subchannel in some way.
/// Producers are inserted into the [`ProducerPolicy`] which is effectively at
/// the root of the LB policy tree, meaning all LB policies are able to see the
/// effects of every producer for subchannels available to those policies.
pub(crate) trait Producer: Any + Send + Sync + Debug + 'static {
    /// Updates [`SubchannelState`] for the [`Subchannel`] it was registered on;
    /// typically by adding an attribute that allows subscribed LB policies to
    /// read the data it produces.
    ///
    /// Note that when a [`Producer`] is dropped, it should ensure that it
    /// releases any resources it acquired and cancel any tasks it spawned.
    fn update_state(&self, state: &mut SubchannelState);
}

/// Holds per-subchannel data for producers.
#[derive(Debug)]
struct SubchannelProducerData {
    /// The inner subchannel returned by the parent channel's `ChannelController`.
    subchannel: Arc<dyn Subchannel>,
    // The live producers for this subchannel.  Note that we do not actively
    // prune values from this map when producers are dropped.  Instead, we will
    // scan it as subchannel updates occur or when new producers are added, at
    // which time we remove entries that cannot be upgraded.
    producers: Mutex<HashMap<TypeId, Weak<dyn Producer>>>,
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

/// Builds a new producer of the given type for the subchannel, or returns an
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
    let producer_work_scheduler = Arc::new(SubchannelProducerWorkScheduler {
        subchannel_data: Arc::downgrade(&attr.subchannel_data),
    });
    let producer = Arc::new(producer_builder.build(
        &attr.subchannel_data.subchannel,
        subchannel_state,
        &(producer_work_scheduler as Arc<dyn WorkScheduler>),
    ));

    producers.insert(
        producer_builder.type_id(),
        Arc::downgrade(&producer) as Weak<dyn Producer>,
    );

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
    /// The inner subchannel returned by the parent channel's `ChannelController`.
    delegate: Arc<dyn Subchannel>,
    dropped_subchannels: Arc<Mutex<HashSet<WeakSubchannel>>>,
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
            .insert((&self.delegate).into());
        self.work_scheduler.schedule_work();
    }
}

/// Wraps a picker returned by the child policy so we can unwrap any picked
/// subchannels before returning them to the channel.
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

/// Holds per-subchannel data and state for single-map lookup.
#[derive(Debug)]
struct SubchannelEntry {
    /// The data shared with producers for this subchannel.
    data: Arc<SubchannelProducerData>,
    /// The latest state of the subchannel known to the delegate.
    state: SubchannelState,
    /// The wrapped delegate subchannel that we returned to the child policy.
    delegate_subchannel: Weak<DelegateSubchannel>,
}

/// Holds all of the shared state that the channel controller and the producer
/// policy need access to.
///
/// Note: all `WeakSubchannel` keys used in maps and sets in this struct refer
/// to the inner subchannels (the ones returned by the parent channel's
/// `ChannelController`), not the wrapped `DelegateSubchannel`s.
#[derive(Debug)]
struct ProducerPolicyState {
    /// A map from weak subchannel to its combined state and data.
    subchannels: HashMap<WeakSubchannel, SubchannelEntry>,
    /// A set of dropped subchannels that we use to clean up any related state.
    dropped_subchannels: Arc<Mutex<HashSet<WeakSubchannel>>>,
    /// The work scheduler we use to schedule work on the child policy.
    work_scheduler: Arc<dyn WorkScheduler>,
}

/// The main Load Balancing Policy for Producers. Intercepts subchannel updates
/// and picker updates to allow producers to intercept calls and track health.
#[derive(Debug)]
pub(crate) struct ProducerPolicy<T: LbPolicyBuilder> {
    delegate: T::LbPolicy,
    delegate_work: Arc<AtomicBool>,
    state: ProducerPolicyState,
}

impl<T: LbPolicyBuilder> ProducerPolicy<T> {
    /// Creates a new `ProducerPolicy` with the given options.
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
            delegate_work,
            state: ProducerPolicyState {
                subchannels: HashMap::default(),
                dropped_subchannels: Arc::new(Mutex::default()),
                work_scheduler,
            },
        }
    }

    /// Processes a subchannel state update. It reconstructs the subchannel state
    /// by combining the latest known state with any updates from active producers,
    /// and then forwards the update to the delegate LB policy.
    fn process_subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        channel_controller: &mut dyn ChannelController,
    ) {
        let mut subchannel_state;
        let subchannel_data;
        {
            let entry = self
                .state
                .subchannels
                .get(&(&subchannel).into())
                .expect("all subchannels should have data added on creation");
            subchannel_data = entry.data.clone();

            // Find alive producers and lock them so we can read them.
            let mut producers = subchannel_data.producers.lock().unwrap();

            let mut alive_producers = Vec::new();
            producers.retain(|_, weak_producer| {
                weak_producer
                    .upgrade()
                    .map(|p| alive_producers.push(p))
                    .is_some()
            });

            // Get the latest known state from the delegate.
            subchannel_state = entry.state.clone();

            // Apply updates from alive producers.
            for producer in alive_producers {
                producer.update_state(&mut subchannel_state);
            }

            // Update state attributes to include policy attribute for new producers.
            subchannel_state.attributes =
                subchannel_state.attributes.add(ProducerPolicyAttribute {
                    subchannel_data: subchannel_data.clone(),
                });
        }

        let delegate_sc = self
            .state
            .subchannels
            .get(&(&subchannel).into())
            .and_then(|entry| entry.delegate_subchannel.upgrade());

        let Some(delegate_sc) = delegate_sc else {
            return;
        };

        let mut wrapped_cc = DelegateChannelController {
            inner: channel_controller,
            state: &mut self.state,
        };

        self.delegate.subchannel_update(
            delegate_sc as Arc<dyn Subchannel>,
            &subchannel_state,
            &mut wrapped_cc,
        );
    }
}

/// Implements [`ChannelController`] when calling the delegate LB policy.
/// Intercepts subchannel creation to add in the [`ProducerPolicy`]'s state and
/// add the subchannel to the maps it stores.  Intercepts picker updates to
/// insert a wrapper that unwraps the picked subchannels.
struct DelegateChannelController<'a> {
    inner: &'a mut dyn ChannelController,
    state: &'a mut ProducerPolicyState,
}

impl<'a> ChannelController for DelegateChannelController<'a> {
    fn new_subchannel(&mut self, address: &Address) -> (Arc<dyn Subchannel>, SubchannelState) {
        let (subchannel, mut state) = self.inner.new_subchannel(address);

        let subchannel_data = Arc::new(SubchannelProducerData {
            producers: Mutex::default(),
            pending_update: AtomicBool::new(false),
            work_scheduler: self.state.work_scheduler.clone(),
            subchannel: subchannel.clone(),
        });
        let subchannel_wrapped = Arc::new(DelegateSubchannel {
            delegate: subchannel.clone(),
            dropped_subchannels: self.state.dropped_subchannels.clone(),
            work_scheduler: self.state.work_scheduler.clone(),
        });

        self.state.subchannels.insert(
            WeakSubchannel::new(&subchannel),
            SubchannelEntry {
                data: subchannel_data.clone(),
                state: state.clone(),
                delegate_subchannel: Arc::downgrade(&subchannel_wrapped),
            },
        );

        state.attributes = state
            .attributes
            .add(ProducerPolicyAttribute { subchannel_data });

        (subchannel_wrapped, state)
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
            state: &mut self.state,
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

        let Some(entry) = self.state.subchannels.get_mut(&weak_sc) else {
            // Subchannel was dropped by the child and we cleaned up its data
            // before the subchannel update occurred.
            return;
        };
        entry.state = state.clone();

        self.process_subchannel_update(subchannel, channel_controller);
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        // First remove data for any dropped subchannels.
        let mut dropped_subchannels = self.state.dropped_subchannels.lock().unwrap();
        for weak_sc in dropped_subchannels.drain() {
            self.state.subchannels.remove(&weak_sc);
        }
        drop(dropped_subchannels);

        // Call the delegate if it requested work.
        if self.delegate_work.swap(false, Ordering::AcqRel) {
            let mut wrapped_cc = DelegateChannelController {
                inner: channel_controller,
                state: &mut self.state,
            };
            self.delegate.work(&mut wrapped_cc);
        }

        // Determine which producers requested work, if any, and call them,
        // calling subchannel_update afterwards.
        let updates: Vec<Arc<dyn Subchannel>> = self
            .state
            .subchannels
            .iter()
            .filter_map(|(sc, entry)| {
                entry
                    .data
                    .pending_update
                    .swap(false, Ordering::AcqRel)
                    .then(|| sc.upgrade())?
            })
            .collect();

        for sc in updates {
            self.process_subchannel_update(sc, channel_controller);
        }
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        let mut wrapped_cc = DelegateChannelController {
            inner: channel_controller,
            state: &mut self.state,
        };
        self.delegate.exit_idle(&mut wrapped_cc);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;

    use super::*;
    use crate::attributes::Attributes;
    use crate::byte_str::ByteStr;
    use crate::client::ConnectivityState;
    use crate::client::load_balancing::LbPolicyOptions;
    use crate::client::load_balancing::OneSubchannelPicker;
    use crate::client::load_balancing::test_utils::*;
    use crate::client::name_resolution::Endpoint;
    use crate::rt::default_runtime;

    #[derive(Debug)]
    struct MockProducer {
        state: AtomicUsize,
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct MockAttribute(usize);

    impl Producer for MockProducer {
        fn update_state(&self, state: &mut SubchannelState) {
            let val = self.state.fetch_add(1, Ordering::SeqCst);
            state.attributes = state.attributes.add(MockAttribute(val));
        }
    }

    #[derive(Debug, Clone)]
    struct MockProducerBuilder {
        id: usize,
        captured_work_scheduler: Arc<Mutex<Option<Arc<dyn WorkScheduler>>>>,
    }

    impl ProducerBuilder for MockProducerBuilder {
        type Producer = MockProducer;

        fn build(
            &self,
            _subchannel: &Arc<dyn Subchannel>,
            _subchannel_state: &SubchannelState,
            work_scheduler: &Arc<dyn WorkScheduler>,
        ) -> Self::Producer {
            *self.captured_work_scheduler.lock().unwrap() = Some(work_scheduler.clone());
            MockProducer {
                state: AtomicUsize::new(0),
            }
        }
    }

    #[derive(Debug)]
    struct MockProducerBuilder2;

    impl ProducerBuilder for MockProducerBuilder2 {
        type Producer = MockProducer;

        fn build(
            &self,
            _subchannel: &Arc<dyn Subchannel>,
            _subchannel_state: &SubchannelState,
            _work_scheduler: &Arc<dyn WorkScheduler>,
        ) -> Self::Producer {
            MockProducer {
                state: AtomicUsize::new(0),
            }
        }
    }

    #[tokio::test]
    async fn test_producer_creation_and_sharing() {
        let (tx, rx) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx.clone(),
        };
        let work_scheduler = Arc::new(TestWorkScheduler { tx_events: tx });

        let lb_options = LbPolicyOptions {
            work_scheduler: work_scheduler.clone(),
            runtime: default_runtime(),
        };

        let captured_state = Arc::new(Mutex::new(None));
        let captured_state_clone = captured_state.clone();

        let active_wrapped_scs: Arc<Mutex<Vec<Arc<dyn Subchannel>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let active_wrapped_scs_clone = active_wrapped_scs.clone();

        let funcs = StubPolicyFuncs {
            resolver_update: Some(Arc::new(move |_data, update, _config, cc| {
                let endpoints = update.endpoints.as_ref().unwrap();
                let addr1 = endpoints[0].addresses[0].clone();
                let (sc1, _state1) = cc.new_subchannel(&addr1);
                active_wrapped_scs_clone.lock().unwrap().push(sc1);
                Ok(())
            })),
            subchannel_update: Some(Arc::new(move |_data, _sc, state, _cc| {
                *captured_state_clone.lock().unwrap() = Some(state.clone());
            })),
            ..Default::default()
        };
        let child_builder = StubPolicyBuilder::new("", funcs);
        let mut producer_policy = ProducerPolicy::new(child_builder, lb_options);

        let mut update = ResolverUpdate::default();
        let addr1 = Address {
            network_type: "tcp",
            address: ByteStr::from("127.0.0.1:8080".to_string()),
            attributes: Attributes::new(),
        };
        update.endpoints = Ok(vec![Endpoint {
            addresses: vec![addr1.clone()],
            attributes: Attributes::new(),
        }]);

        producer_policy
            .resolver_update(update, None, &mut cc)
            .unwrap();

        let TestEvent::NewSubchannel(sc1) = rx.try_recv().unwrap() else {
            panic!()
        };

        let state1 = SubchannelState::idle();

        // Emulate subchannel_update hitting ProducerPolicy to populate maps
        producer_policy.subchannel_update(sc1.clone(), &state1, &mut cc);

        let state_to_use = captured_state.lock().unwrap().clone().unwrap();

        let captured_ws = Arc::new(Mutex::new(None));
        let builder1 = MockProducerBuilder {
            id: 1,
            captured_work_scheduler: captured_ws.clone(),
        };
        let builder2 = MockProducerBuilder2;

        let p1 = get_or_build_producer(builder1.clone(), &state_to_use);
        let p2 = get_or_build_producer(
            MockProducerBuilder {
                id: 1,
                captured_work_scheduler: captured_ws.clone(),
            },
            &state_to_use,
        );
        let p3 = get_or_build_producer(builder2, &state_to_use);

        assert!(
            Arc::ptr_eq(&p1, &p2),
            "same builder type should share producer"
        );
        assert!(
            !Arc::ptr_eq(&p1, &p3),
            "different builder type should create new producer"
        );
    }

    #[tokio::test]
    async fn test_producer_work_scheduling() {
        let (tx, rx) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx.clone(),
        };
        let work_scheduler = Arc::new(TestWorkScheduler { tx_events: tx });

        let lb_options = LbPolicyOptions {
            work_scheduler: work_scheduler.clone(),
            runtime: default_runtime(),
        };

        let captured_state = Arc::new(Mutex::new(None));
        let captured_state_clone = captured_state.clone();

        let active_wrapped_scs: Arc<Mutex<Vec<Arc<dyn Subchannel>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let active_wrapped_scs_clone = active_wrapped_scs.clone();

        let funcs = StubPolicyFuncs {
            resolver_update: Some(Arc::new(move |_data, update, _config, cc| {
                let endpoints = update.endpoints.as_ref().unwrap();
                let addr1 = endpoints[0].addresses[0].clone();
                let (sc1, _state1) = cc.new_subchannel(&addr1);
                active_wrapped_scs_clone.lock().unwrap().push(sc1);
                Ok(())
            })),
            subchannel_update: Some(Arc::new(move |_data, _sc, state, _cc| {
                *captured_state_clone.lock().unwrap() = Some(state.clone());
            })),
            ..Default::default()
        };
        let child_builder = StubPolicyBuilder::new("", funcs);
        let mut producer_policy = ProducerPolicy::new(child_builder, lb_options);

        let mut update = ResolverUpdate::default();
        let addr1 = Address {
            network_type: "tcp",
            address: ByteStr::from("127.0.0.1:8080".to_string()),
            attributes: Attributes::new(),
        };
        update.endpoints = Ok(vec![Endpoint {
            addresses: vec![addr1.clone()],
            attributes: Attributes::new(),
        }]);

        producer_policy
            .resolver_update(update, None, &mut cc)
            .unwrap();

        let TestEvent::NewSubchannel(sc1) = rx.try_recv().unwrap() else {
            panic!()
        };

        let state1 = SubchannelState::idle();

        producer_policy.subchannel_update(sc1.clone(), &state1, &mut cc);

        // Capture state_to_use so we have access to the WorkScheduler!
        let state_to_use = captured_state.lock().unwrap().clone().unwrap();

        let captured_ws = Arc::new(Mutex::new(None));
        let builder = MockProducerBuilder {
            id: 1,
            captured_work_scheduler: captured_ws.clone(),
        };
        let producer = get_or_build_producer(builder, &state_to_use);

        // Verify initial state has MockAttribute(0)
        let mut initial_state = SubchannelState::idle();
        producer.update_state(&mut initial_state);
        assert_eq!(
            initial_state.attributes.get::<MockAttribute>(),
            Some(&MockAttribute(0))
        );

        // Trigger work via the captured WorkScheduler
        let ws = captured_ws.lock().unwrap().clone().unwrap();
        ws.schedule_work();

        // Expect ScheduleWork in rx
        assert_eq!(rx.try_recv(), Ok(TestEvent::ScheduleWork));

        // Call work on ProducerPolicy
        producer_policy.work(&mut cc);

        // Expect process_subchannel_update to be called!
        // Which means subchannel_update on the delegate will be called!
        // And captured_state will be updated with MockAttribute(1) !
        let updated_state = captured_state.lock().unwrap().clone().unwrap();
        assert_eq!(
            updated_state.attributes.get::<MockAttribute>(),
            Some(&MockAttribute(1))
        );

        // Reset captured state
        *captured_state.lock().unwrap() = None;

        // Call work again without schedule_work()
        producer_policy.work(&mut cc);

        // Verify it was NOT updated!
        assert!(
            captured_state.lock().unwrap().is_none(),
            "Should not update without new schedule_work()"
        );
    }

    #[tokio::test]
    async fn test_channel_updates_forwarding() {
        let (tx, rx) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx.clone(),
        };
        let work_scheduler = Arc::new(TestWorkScheduler { tx_events: tx });

        let lb_options = LbPolicyOptions {
            work_scheduler: work_scheduler.clone(),
            runtime: default_runtime(),
        };

        let captured_state = Arc::new(Mutex::new(None));
        let captured_state_clone = captured_state.clone();

        let active_wrapped_scs: Arc<Mutex<Vec<Arc<dyn Subchannel>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let active_wrapped_scs_clone = active_wrapped_scs.clone();

        let funcs = StubPolicyFuncs {
            resolver_update: Some(Arc::new(move |_data, update, _config, cc| {
                let endpoints = update.endpoints.as_ref().unwrap();
                let addr1 = endpoints[0].addresses[0].clone();
                let (sc1, _state1) = cc.new_subchannel(&addr1);
                active_wrapped_scs_clone.lock().unwrap().push(sc1);
                Ok(())
            })),
            subchannel_update: Some(Arc::new(move |_data, _sc, state, cc| {
                *captured_state_clone.lock().unwrap() = Some(state.clone());
                let addr = Address {
                    network_type: "tcp",
                    address: ByteStr::from("127.0.0.1:8082".to_string()),
                    attributes: Attributes::new(),
                };
                let (_sc, state) = cc.new_subchannel(&addr);
                assert!(state.attributes.get::<ProducerPolicyAttribute>().is_some());
            })),
            ..Default::default()
        };
        let child_builder = StubPolicyBuilder::new("", funcs);
        let mut producer_policy = ProducerPolicy::new(child_builder, lb_options);

        let mut update = ResolverUpdate::default();
        let addr1 = Address {
            network_type: "tcp",
            address: ByteStr::from("127.0.0.1:8080".to_string()),
            attributes: Attributes::new(),
        };
        update.endpoints = Ok(vec![Endpoint {
            addresses: vec![addr1.clone()],
            attributes: crate::attributes::Attributes::new(),
        }]);

        producer_policy
            .resolver_update(update, None, &mut cc)
            .unwrap();

        let TestEvent::NewSubchannel(sc1) = rx.try_recv().unwrap() else {
            panic!()
        };

        let state1 = SubchannelState::idle();

        producer_policy.subchannel_update(sc1.clone(), &state1, &mut cc);

        // Verify initial forward
        let forward1 = captured_state.lock().unwrap().clone().unwrap();
        assert_eq!(forward1.connectivity_state, ConnectivityState::Idle);

        // Emulate connectivity state change from parent channel
        let state2 = SubchannelState {
            connectivity_state: ConnectivityState::Ready,
            last_connection_error: None,
            attributes: crate::attributes::Attributes::new(),
        };
        producer_policy.subchannel_update(sc1.clone(), &state2, &mut cc);

        // Verify child sees Ready
        let forward2 = captured_state.lock().unwrap().clone().unwrap();
        assert_eq!(forward2.connectivity_state, ConnectivityState::Ready);
        assert!(
            forward2
                .attributes
                .get::<crate::client::load_balancing::producer::ProducerPolicyAttribute>()
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_cleanup_on_subchannel_drop() {
        let (tx, rx) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx.clone(),
        };
        let work_scheduler = Arc::new(TestWorkScheduler { tx_events: tx });

        let lb_options = LbPolicyOptions {
            work_scheduler: work_scheduler.clone(),
            runtime: default_runtime(),
        };

        let active_wrapped_scs: Arc<Mutex<Vec<Arc<dyn Subchannel>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let active_wrapped_scs_clone = active_wrapped_scs.clone();

        let funcs = StubPolicyFuncs {
            resolver_update: Some(Arc::new(move |_data, update, _config, cc| {
                let endpoints = update.endpoints.as_ref().unwrap();
                let addr1 = endpoints[0].addresses[0].clone();
                let (sc1, _state1) = cc.new_subchannel(&addr1);
                active_wrapped_scs_clone.lock().unwrap().push(sc1);
                Ok(())
            })),
            ..Default::default()
        };
        let child_builder = StubPolicyBuilder::new("", funcs);
        let mut producer_policy = ProducerPolicy::new(child_builder, lb_options);

        let mut update = ResolverUpdate::default();
        let addr1 = Address {
            network_type: "tcp",
            address: ByteStr::from("127.0.0.1:8080".to_string()),
            attributes: Attributes::new(),
        };
        update.endpoints = Ok(vec![Endpoint {
            addresses: vec![addr1.clone()],
            attributes: Attributes::new(),
        }]);

        producer_policy
            .resolver_update(update, None, &mut cc)
            .unwrap();

        let TestEvent::NewSubchannel(sc1_inner) = rx.try_recv().unwrap() else {
            panic!()
        };

        let state1 = SubchannelState::idle();
        producer_policy.subchannel_update(sc1_inner.clone(), &state1, &mut cc);

        // Verify it was inserted
        let weak_inner = WeakSubchannel::new(&sc1_inner);
        assert!(producer_policy.state.subchannels.contains_key(&weak_inner));

        // Drop the wrapped subchannel
        active_wrapped_scs.lock().unwrap().clear();

        // Call work to trigger cleanup
        producer_policy.work(&mut cc);

        // Verify it was removed
        assert!(!producer_policy.state.subchannels.contains_key(&weak_inner));
    }

    #[tokio::test]
    async fn test_cleanup_on_producer_drop() {
        let (tx, rx) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx.clone(),
        };
        let work_scheduler = Arc::new(TestWorkScheduler { tx_events: tx });

        let lb_options = LbPolicyOptions {
            work_scheduler: work_scheduler.clone(),
            runtime: default_runtime(),
        };

        let captured_state = Arc::new(Mutex::new(None));
        let captured_state_clone = captured_state.clone();

        let active_wrapped_scs: Arc<Mutex<Vec<Arc<dyn Subchannel>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let active_wrapped_scs_clone = active_wrapped_scs.clone();

        let funcs = StubPolicyFuncs {
            resolver_update: Some(Arc::new(move |_data, update, _config, cc| {
                let endpoints = update.endpoints.as_ref().unwrap();
                let addr1 = endpoints[0].addresses[0].clone();
                let (sc1, _state1) = cc.new_subchannel(&addr1);
                active_wrapped_scs_clone.lock().unwrap().push(sc1);
                Ok(())
            })),
            subchannel_update: Some(Arc::new(move |_data, _sc, state, _cc| {
                *captured_state_clone.lock().unwrap() = Some(state.clone());
            })),
            ..Default::default()
        };
        let child_builder = StubPolicyBuilder::new("", funcs);
        let mut producer_policy = ProducerPolicy::new(child_builder, lb_options);

        let mut update = ResolverUpdate::default();
        let addr1 = Address {
            network_type: "tcp",
            address: ByteStr::from("127.0.0.1:8080".to_string()),
            attributes: Attributes::new(),
        };
        update.endpoints = Ok(vec![Endpoint {
            addresses: vec![addr1.clone()],
            attributes: Attributes::new(),
        }]);

        producer_policy
            .resolver_update(update, None, &mut cc)
            .unwrap();

        let TestEvent::NewSubchannel(sc1_inner) = rx.try_recv().unwrap() else {
            panic!()
        };

        let state1 = SubchannelState::idle();
        producer_policy.subchannel_update(sc1_inner.clone(), &state1, &mut cc);

        let state_to_use = captured_state.lock().unwrap().clone().unwrap();

        let captured_ws = Arc::new(Mutex::new(None));
        let builder = MockProducerBuilder {
            id: 1,
            captured_work_scheduler: captured_ws.clone(),
        };

        let p1 = get_or_build_producer(builder.clone(), &state_to_use);
        let p2 = get_or_build_producer(builder.clone(), &state_to_use);
        assert!(Arc::ptr_eq(&p1, &p2));

        let p2_ptr = Arc::as_ptr(&p2);

        drop(p1);
        drop(p2);

        let p3 = get_or_build_producer(builder.clone(), &state_to_use);
        assert_ne!(
            Arc::as_ptr(&p3),
            p2_ptr,
            "Producer should be recreated after all references are dropped"
        );
    }

    #[tokio::test]
    async fn test_delegate_work_scheduling() {
        let (tx, rx) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx.clone(),
        };
        let work_scheduler = Arc::new(TestWorkScheduler { tx_events: tx });

        let lb_options = LbPolicyOptions {
            work_scheduler: work_scheduler.clone(),
            runtime: default_runtime(),
        };

        let captured_ws = Arc::new(Mutex::new(None));
        let captured_ws_clone = captured_ws.clone();

        let delegate_work_called = Arc::new(Mutex::new(false));
        let delegate_work_called_clone = delegate_work_called.clone();

        let funcs = StubPolicyFuncs {
            resolver_update: Some(Arc::new(move |data, _update, _config, _cc| {
                *captured_ws_clone.lock().unwrap() =
                    Some(data.lb_policy_options.work_scheduler.clone());
                Ok(())
            })),
            work: Some(Arc::new(move |_data, cc| {
                let addr = Address {
                    network_type: "tcp",
                    address: ByteStr::from("127.0.0.1:8081".to_string()),
                    attributes: Attributes::new(),
                };
                let (_sc, state) = cc.new_subchannel(&addr);
                assert!(state.attributes.get::<ProducerPolicyAttribute>().is_some());
                *delegate_work_called_clone.lock().unwrap() = true;
            })),
            ..Default::default()
        };
        let child_builder = StubPolicyBuilder::new("", funcs);
        let mut producer_policy = ProducerPolicy::new(child_builder, lb_options);

        // Call resolver_update to trigger the capture of the delegate's WorkScheduler
        let update = ResolverUpdate::default();
        producer_policy
            .resolver_update(update, None, &mut cc)
            .unwrap();

        // Get the WorkScheduler passed to the delegate
        let ws = captured_ws.lock().unwrap().clone().unwrap();

        // Call schedule_work on it
        ws.schedule_work();

        // Verify it sent ScheduleWork up to the parent channel
        assert_eq!(rx.try_recv(), Ok(TestEvent::ScheduleWork));

        // Call work on ProducerPolicy
        producer_policy.work(&mut cc);

        // Verify the delegate's work method was called!
        assert!(
            *delegate_work_called.lock().unwrap(),
            "Delegate work should be called when it schedules work"
        );

        // Reset delegate work called flag
        *delegate_work_called.lock().unwrap() = false;

        // Call work again without schedule_work()
        producer_policy.work(&mut cc);

        // Verify it was NOT called!
        assert!(
            !*delegate_work_called.lock().unwrap(),
            "Delegate work should NOT be called twice without new schedule_work()"
        );
    }

    #[tokio::test]
    async fn test_picker_unwrapping() {
        let (tx, rx) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx.clone(),
        };
        let work_scheduler = Arc::new(TestWorkScheduler { tx_events: tx });

        let lb_options = LbPolicyOptions {
            work_scheduler: work_scheduler.clone(),
            runtime: default_runtime(),
        };

        let funcs = StubPolicyFuncs {
            resolver_update: Some(Arc::new(move |_data, update, _config, cc| {
                let endpoints = update.endpoints.as_ref().unwrap();
                let addr1 = endpoints[0].addresses[0].clone();
                let (sc1, _state1) = cc.new_subchannel(&addr1);

                // Emulate updating picker from child policy!
                let picker = Arc::new(OneSubchannelPicker { sc: sc1.clone() });
                cc.update_picker(LbState {
                    connectivity_state: ConnectivityState::Ready,
                    picker,
                });

                Ok(())
            })),
            ..Default::default()
        };
        let child_builder = StubPolicyBuilder::new("test", funcs);
        let mut producer_policy = ProducerPolicy::new(child_builder, lb_options);

        let mut update = ResolverUpdate::default();
        let addr1 = Address {
            network_type: "tcp",
            address: ByteStr::from("127.0.0.1:8080".to_string()),
            attributes: Attributes::new(),
        };
        update.endpoints = Ok(vec![Endpoint {
            addresses: vec![addr1.clone()],
            attributes: Attributes::new(),
        }]);

        producer_policy
            .resolver_update(update, None, &mut cc)
            .unwrap();

        // Expect NewSubchannel
        let TestEvent::NewSubchannel(sc1_inner) = rx.try_recv().unwrap() else {
            panic!()
        };

        // Expect UpdatePicker
        let TestEvent::UpdatePicker(lb_state) = rx.try_recv().unwrap() else {
            panic!()
        };

        // The picker should be a DelegatePicker!
        let picker = lb_state.picker;

        let request = RequestHeaders::new();
        let pick_result = picker.pick(&request);

        if let PickResult::Pick(pick) = pick_result {
            // Verify it was unwrapped to the INNER subchannel!
            assert!(Arc::ptr_eq(&pick.subchannel, &sc1_inner));
        } else {
            panic!("Expected PickResult::Pick");
        }
    }

    #[tokio::test]
    async fn test_exit_idle_wrapping() {
        let (tx, rx) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx.clone(),
        };
        let work_scheduler = Arc::new(TestWorkScheduler { tx_events: tx });

        let lb_options = LbPolicyOptions {
            work_scheduler: work_scheduler.clone(),
            runtime: default_runtime(),
        };

        let exit_idle_called = Arc::new(Mutex::new(false));
        let exit_idle_called_clone = exit_idle_called.clone();

        let funcs = StubPolicyFuncs {
            exit_idle: Some(Arc::new(move |_data, cc| {
                let addr = Address {
                    network_type: "tcp",
                    address: ByteStr::from("127.0.0.1:8083".to_string()),
                    attributes: Attributes::new(),
                };
                let (_sc, state) = cc.new_subchannel(&addr);
                assert!(state.attributes.get::<ProducerPolicyAttribute>().is_some());
                *exit_idle_called_clone.lock().unwrap() = true;
            })),
            ..Default::default()
        };
        let child_builder = StubPolicyBuilder::new("test", funcs);
        let mut producer_policy = ProducerPolicy::new(child_builder, lb_options);

        // Call exit_idle on ProducerPolicy
        producer_policy.exit_idle(&mut cc);

        // Verify it was called
        assert!(
            *exit_idle_called.lock().unwrap(),
            "Delegate exit_idle should be called"
        );

        // And verify that NewSubchannel was sent up to our TestChannelController!
        let event = rx.try_recv().unwrap();
        if let TestEvent::NewSubchannel(_) = event {
            // ok!
        } else {
            panic!("Expected NewSubchannel event from exit_idle");
        }
    }
}
