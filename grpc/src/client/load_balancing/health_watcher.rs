use std::sync::Arc;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::Weak;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::Ordering;

use crate::client::load_balancing::WorkScheduler;
use crate::client::load_balancing::producer;
use crate::client::load_balancing::producer::Producer;
use crate::client::load_balancing::producer::ProducerBuilder;
use crate::client::load_balancing::subchannel::Subchannel;
use crate::client::load_balancing::subchannel::SubchannelState;

/// The connection health state of a subchannel.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum HealthState {
    Healthy,
    Unhealthy,
    Unknown,
}

/// An attribute added to the subchannel state to store its health.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct HealthAttribute(HealthState);

/// Allows a parent LB policy to explicitly subscribe a Subchannel to background Health checking
/// without knowing about the underlying ProducerPolicy execution framework logic!
pub(crate) fn subscribe(state: &SubchannelState) -> Arc<HealthWatcher> {
    #[cfg(test)]
    let builder = HealthWatcherBuilder { drop_tracker: None };
    #[cfg(not(test))]
    let builder = HealthWatcherBuilder;
    producer::get_or_build_producer(builder, state)
}

/// Retrieves the health state from the subchannel state attributes if it exists.
pub(crate) fn get_health(state: &SubchannelState) -> Option<HealthState> {
    state
        .attributes
        .get::<HealthAttribute>()
        .map(|h| h.0.clone())
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct HealthWatcherBuilder {
    pub(crate) drop_tracker: Option<Weak<AtomicBool>>,
}

#[cfg(not(test))]
#[derive(Debug)]
pub(crate) struct HealthWatcherBuilder;

impl ProducerBuilder for HealthWatcherBuilder {
    type Producer = HealthWatcher;

    fn build(
        &self,
        subchannel: &Arc<dyn Subchannel>,
        subchannel_state: &SubchannelState,
        work_scheduler: &Arc<dyn WorkScheduler>,
    ) -> Self::Producer {
        let provider = HealthWatcher {
            health_state: Mutex::new(HealthState::Unknown),
            work_scheduler: work_scheduler.clone(),
            #[cfg(test)]
            drop_tracker: self.drop_tracker.clone(),
        };
        provider.start_rpc();
        provider
    }
}

/// A producer that watches the health of a subchannel and updates its state.
#[derive(Debug)]
pub(crate) struct HealthWatcher {
    /// The current health state.
    health_state: Mutex<HealthState>,
    /// The work scheduler we use to notify the policy when health changes.
    work_scheduler: Arc<dyn WorkScheduler>,
    #[cfg(test)]
    pub(crate) drop_tracker: Option<Weak<AtomicBool>>,
}

impl HealthWatcher {
    fn start_rpc(&self) {
        // [STUB] Spawn background task, fetch metadata loop natively checking health
    }

    /// Sets the health state and schedules work to notify the policy.
    pub(crate) fn set_health(&self, state: HealthState) {
        *self.health_state.lock().unwrap() = state;
        self.work_scheduler.schedule_work();
    }
}

impl Drop for HealthWatcher {
    fn drop(&mut self) {
        #[cfg(test)]
        if let Some(tracker) = &self.drop_tracker
            && let Some(tracker) = tracker.upgrade()
        {
            tracker.store(true, Ordering::Relaxed);
        }
        // [STUB] Notify cancellation to active RPC loop safely dropping resources.
    }
}

impl Producer for HealthWatcher {
    fn update_state(&self, state: &mut SubchannelState) {
        let health = self.health_state.lock().unwrap().clone();
        state.attributes = state.attributes.add(HealthAttribute(health));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::Mutex;
    use std::sync::mpsc;

    use super::*;
    use crate::attributes::Attributes;
    use crate::byte_str::ByteStr;
    use crate::client::ConnectivityState;
    use crate::client::load_balancing::LbPolicy;
    use crate::client::load_balancing::LbPolicyOptions;
    use crate::client::load_balancing::producer::ProducerPolicy;
    use crate::client::load_balancing::subchannel::SubchannelState;
    use crate::client::load_balancing::test_utils::StubPolicyBuilder;
    use crate::client::load_balancing::test_utils::StubPolicyFuncs;
    use crate::client::load_balancing::test_utils::TestChannelController;
    use crate::client::load_balancing::test_utils::TestEvent;
    use crate::client::load_balancing::test_utils::TestWorkScheduler;
    use crate::client::name_resolution::Address;
    use crate::client::name_resolution::Endpoint;
    use crate::client::name_resolution::ResolverUpdate;

    #[tokio::test]
    async fn test_health_watcher() {
        let last_health1 = Arc::new(Mutex::new(Some(HealthState::Unknown)));
        let last_health1_clone = last_health1.clone();

        let drop_tracker = Arc::new(AtomicBool::new(false));
        let drop_tracker_clone = drop_tracker.clone();

        let hw1_ref: Arc<Mutex<Option<Arc<HealthWatcher>>>> = Arc::new(Mutex::new(None));
        let hw1_ref_clone = hw1_ref.clone();

        // Must preserve exact variables cleanly to simulate active policy states.
        let sc1_ref: Arc<Mutex<Option<Arc<dyn Subchannel>>>> = Arc::new(Mutex::new(None));
        let sc2_ref: Arc<Mutex<Option<Arc<dyn Subchannel>>>> = Arc::new(Mutex::new(None));

        let sc1_ref_clone = sc1_ref.clone();
        let sc2_ref_clone = sc2_ref.clone();

        let funcs = StubPolicyFuncs {
            resolver_update: Some(Arc::new(move |_data, update, _config, cc| {
                let endpoints = update.endpoints.as_ref().unwrap();
                let addr1 = endpoints[0].addresses[0].clone();
                let addr2 = endpoints[0].addresses[1].clone();

                let (sc1, state1) = cc.new_subchannel(&addr1);
                let (sc2, _state2) = cc.new_subchannel(&addr2);

                *sc1_ref_clone.lock().unwrap() = Some(sc1.clone());
                *sc2_ref_clone.lock().unwrap() = Some(sc2.clone());

                // We dynamically subscribe without manually injecting structures
                let builder = HealthWatcherBuilder {
                    drop_tracker: Some(std::sync::Arc::downgrade(&drop_tracker_clone)),
                };
                let hw = producer::get_or_build_producer(builder, &state1);
                *hw1_ref_clone.lock().unwrap() = Some(hw);

                Ok(())
            })),
            subchannel_update: Some(Arc::new(move |_data, sc, state, _cc| {
                // Determine whether this update is for sc1 or sc2 sequentially
                if sc.address().address.to_string() == "127.0.0.1:8080" {
                    *last_health1_clone.lock().unwrap() = get_health(state);
                } else if sc.address().address.to_string() == "10.0.0.1:8081" {
                    assert_eq!(
                        get_health(state),
                        None,
                        "sc2 is unwatched; it should not resolve a valid health state."
                    );
                }
            })),
            ..Default::default()
        };

        let (tx, rx) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx.clone(),
        };
        let work_scheduler = Arc::new(TestWorkScheduler { tx_events: tx });

        let lb_options = LbPolicyOptions {
            work_scheduler: work_scheduler.clone(),
            runtime: crate::rt::default_runtime(),
        };

        let child_builder = StubPolicyBuilder::new("", funcs);

        let mut producer = ProducerPolicy::new(child_builder, lb_options);

        // Bootstrap: push TWO endpoints generating creation intercept calls
        let mut update = ResolverUpdate::default();
        let addr1 = Address {
            network_type: "tcp",
            address: ByteStr::from("127.0.0.1:8080".to_string()),
            attributes: Attributes::new(),
        };
        let addr2 = Address {
            network_type: "tcp",
            address: ByteStr::from("10.0.0.1:8081".to_string()),
            attributes: Attributes::new(),
        };
        update.endpoints = Ok(vec![Endpoint {
            addresses: vec![addr1, addr2],
            attributes: Attributes::new(),
        }]);

        producer.resolver_update(update, None, &mut cc).unwrap();

        let TestEvent::NewSubchannel(sc1) = rx.try_recv().unwrap() else {
            panic!()
        };
        let TestEvent::NewSubchannel(sc2) = rx.try_recv().unwrap() else {
            panic!()
        };

        // Let the state map formally establishing Provider frameworks universally
        let state1 = SubchannelState {
            connectivity_state: ConnectivityState::Ready,
            last_connection_error: None,
            attributes: Attributes::new(),
        };
        producer.subchannel_update(sc1.clone(), &state1, &mut cc);
        producer.subchannel_update(sc2.clone(), &state1, &mut cc);

        // Verify sc1 mapped exactly to `Unknown` based on normal background hook logic
        assert_eq!(*last_health1.lock().unwrap(), Some(HealthState::Unknown));

        // Inject out of band change mirroring RPC hook mapping directly to watched channel
        {
            let hw1 = hw1_ref.lock().unwrap().clone().unwrap();
            hw1.set_health(HealthState::Healthy);
        }

        // Await strictly over out of bounds event triggered by .schedule_work() inside provider
        assert_eq!(rx.try_recv(), Ok(TestEvent::ScheduleWork));

        // Cycle explicit `.work()` wrapper pushing state natively forward towards children
        producer.work(&mut cc);

        // Expect state changed predictably!
        assert_eq!(*last_health1.lock().unwrap(), Some(HealthState::Healthy));

        // Assert queue did not get populated!
        assert!(
            rx.try_recv().is_err(),
            "Unwatched streams shouldn't schedule updates"
        );

        // Finalize: drops pointers seamlessly cancelling Providers!
        sc1_ref.lock().unwrap().take();
        sc2_ref.lock().unwrap().take();
        hw1_ref.lock().unwrap().take();

        assert_eq!(rx.try_recv(), Ok(TestEvent::ScheduleWork));
        producer.work(&mut cc);

        assert!(drop_tracker.load(Ordering::Relaxed));
    }
}
