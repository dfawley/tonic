/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::client::load_balancing::ChannelController;
use crate::client::load_balancing::LbPolicy;
use crate::client::load_balancing::LbState;
use crate::client::load_balancing::WorkData;
use crate::client::load_balancing::WorkScheduler;
use crate::client::load_balancing::subchannel::CancelToken;
use crate::client::load_balancing::subchannel::Subchannel;
use crate::client::load_balancing::subchannel::SubchannelState;
use crate::client::load_balancing::subchannel::WeakSubchannel;
use crate::client::name_resolution::Address;
use crate::client::name_resolution::ResolverUpdate;

/// Implements subchannel sharing for T.  Whenever T creates a subchannel, this
/// policy returns the subchannel directly, and if another subchannel is created
/// for the same address, the first subchannel will be reused to back the second
/// subchannel.
#[derive(Debug)]
pub(crate) struct SubchannelSharing<T> {
    delegate: T,
    subchannels: HashMap<Address, SharedSubchannel>,
    work_scheduler: Arc<dyn WorkScheduler>,
}

impl<T> SubchannelSharing<T> {
    pub(crate) fn new(delegate: T, work_scheduler: Arc<dyn WorkScheduler>) -> Self {
        Self {
            delegate,
            subchannels: HashMap::new(),
            work_scheduler,
        }
    }
}

struct SharedSubchannel {
    subchannel: WeakSubchannel,
    state: SubchannelState,
    _handle: Box<dyn CancelToken>,
}

impl Debug for SharedSubchannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSubchannel")
            .field("subchannel", &self.subchannel)
            .field("state", &self.state)
            .finish()
    }
}

#[derive(Debug)]
struct StateUpdate {
    address: Address,
    state: SubchannelState,
}

impl<T: LbPolicy> LbPolicy for SubchannelSharing<T> {
    type LbConfig = T::LbConfig;

    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&T::LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), String> {
        let mut channel_controller = SharingChannelController {
            subchannels: &mut self.subchannels,
            work_scheduler: self.work_scheduler.clone(),
            delegate: channel_controller,
        };
        self.delegate
            .resolver_update(update, config, &mut channel_controller)
    }

    fn work(&mut self, data: Option<WorkData>, channel_controller: &mut dyn ChannelController) {
        if let Some(d) = data.as_ref().and_then(|d| d.downcast_ref::<StateUpdate>()) {
            if let Some(shared) = self.subchannels.get_mut(&d.address) {
                shared.state = d.state.clone();
            }
            return;
        }

        let mut channel_controller = SharingChannelController {
            subchannels: &mut self.subchannels,
            work_scheduler: self.work_scheduler.clone(),
            delegate: channel_controller,
        };
        self.delegate.work(data, &mut channel_controller);
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        let mut channel_controller = SharingChannelController {
            subchannels: &mut self.subchannels,
            work_scheduler: self.work_scheduler.clone(),
            delegate: channel_controller,
        };
        self.delegate.exit_idle(&mut channel_controller);
    }
}

struct SharingChannelController<'a> {
    subchannels: &'a mut HashMap<Address, SharedSubchannel>,
    work_scheduler: Arc<dyn WorkScheduler>,
    delegate: &'a mut dyn ChannelController,
}

impl<'a> ChannelController for SharingChannelController<'a> {
    fn new_subchannel(&mut self, address: &Address) -> (Arc<dyn Subchannel>, SubchannelState) {
        self.subchannels
            .retain(|_, shared| shared.subchannel.strong_count() > 0);

        if let Some(shared) = self.subchannels.get(address)
            && let Some(int_subchannel) = shared.subchannel.upgrade()
        {
            return (int_subchannel, shared.state.clone());
        }

        let (int_subchannel, state) = self.delegate.new_subchannel(address);
        let weak_sc = WeakSubchannel::new(&int_subchannel);

        let work_scheduler = self.work_scheduler.clone();
        let address_clone = address.clone();
        let handle = int_subchannel.subscribe(move |state: SubchannelState| {
            work_scheduler.schedule_work(Some(Box::new(StateUpdate {
                address: address_clone.clone(),
                state,
            })));
        });

        self.subchannels.insert(
            address.clone(),
            SharedSubchannel {
                subchannel: weak_sc,
                state: state.clone(),
                _handle: handle,
            },
        );

        (int_subchannel, state)
    }

    fn update_picker(&mut self, update: LbState) {
        self.delegate.update_picker(update);
    }

    fn request_resolution(&mut self) {
        self.delegate.request_resolution();
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::mpsc;

    use super::*;
    use crate::client::ConnectivityState;
    use crate::client::load_balancing::LbPolicy;
    use crate::client::load_balancing::LbPolicyOptions;
    use crate::client::load_balancing::Pick;
    use crate::client::load_balancing::PickResult;
    use crate::client::load_balancing::Picker;
    use crate::client::load_balancing::subchannel::SubchannelState;
    use crate::client::load_balancing::test_utils::StubPolicy;
    use crate::client::load_balancing::test_utils::StubPolicyFuncs;
    use crate::client::load_balancing::test_utils::TestChannelController;
    use crate::client::load_balancing::test_utils::TestEvent;
    use crate::client::load_balancing::test_utils::TestWorkScheduler;
    use crate::client::load_balancing::test_utils::new_request_headers;
    use crate::client::name_resolution::Address;
    use crate::client::name_resolution::ResolverUpdate;
    use crate::core::RequestHeaders;
    use crate::metadata::MetadataMap;
    use crate::rt::default_runtime;

    fn test_lb_policy_options(tx_events: mpsc::Sender<TestEvent>) -> LbPolicyOptions {
        LbPolicyOptions {
            work_scheduler: Arc::new(TestWorkScheduler::new(tx_events)),
            runtime: default_runtime(),
        }
    }

    // Tests that a single subchannel creation is properly forwarded to the
    // underlying channel controller and the created shared subchannel seen by
    // the delegate policy contains the real one.
    #[test]
    fn test_single_subchannel() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let sc_out = Arc::new(Mutex::new(None));
        let sc_out_clone = sc_out.clone();

        let opts = test_lb_policy_options(tx_events.clone());
        let ws = opts.work_scheduler.clone();
        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |_data, _workitem, cc| {
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    let sc = cc.new_subchannel(&addr).0;
                    *sc_out_clone.lock().unwrap() = Some(sc);
                })),
                ..Default::default()
            },
            opts,
        );

        let mut sharing = SubchannelSharing::new(mock, ws);

        sharing.work(None, &mut cc);

        let event = rx_events.recv().unwrap();
        let TestEvent::NewSubchannel(internal_sc) = event else {
            panic!("expected NewSubchannel")
        };

        let external_sc = sc_out.lock().unwrap().take().unwrap();
        assert!(Arc::ptr_eq(&external_sc, &internal_sc));
    }

    // Tests that when a delegate policy creates multiple subchannels with the
    // same address, they share the same delegate subchannel from the underlying
    // channel controller.
    #[test]
    fn test_multiple_subchannels_same_address() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let sc_out1 = Arc::new(Mutex::new(None));
        let sc_out1_clone = sc_out1.clone();
        let sc_out2 = Arc::new(Mutex::new(None));
        let sc_out2_clone = sc_out2.clone();

        let opts = test_lb_policy_options(tx_events.clone());
        let ws = opts.work_scheduler.clone();
        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |_data, _workitem, cc| {
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    *sc_out1_clone.lock().unwrap() = Some(cc.new_subchannel(&addr).0);
                    *sc_out2_clone.lock().unwrap() = Some(cc.new_subchannel(&addr).0);
                })),
                ..Default::default()
            },
            opts,
        );

        let mut sharing = SubchannelSharing::new(mock, ws);

        sharing.work(None, &mut cc);

        // Confirm that only one new_subchannel was seen by the underlying
        // channel controller.
        let event = rx_events.recv().unwrap();
        let TestEvent::NewSubchannel(internal_sc) = event else {
            panic!("expected NewSubchannel")
        };
        assert!(rx_events.try_recv().is_err());

        // Confirm that both subchannels seen by the delegate share the same
        // underlying subchannel.
        let external_sc1 = sc_out1.lock().unwrap().take().unwrap();
        let external_sc2 = sc_out2.lock().unwrap().take().unwrap();

        assert!(Arc::ptr_eq(&external_sc1, &internal_sc));
        assert!(Arc::ptr_eq(&external_sc2, &internal_sc));
    }

    // Tests that when the delegate creates subchannels with different
    // addresses, they get different internal subchannels.
    #[test]
    fn test_multiple_subchannels_different_addresses() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let sc_out1 = Arc::new(Mutex::new(None));
        let sc_out1_clone = sc_out1.clone();
        let sc_out2 = Arc::new(Mutex::new(None));
        let sc_out2_clone = sc_out2.clone();

        let opts = test_lb_policy_options(tx_events.clone());
        let ws = opts.work_scheduler.clone();
        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |_data, _workitem, cc| {
                    let addr1 = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    let addr2 = Address {
                        address: "127.0.0.2:80".to_string().into(),
                        ..Default::default()
                    };
                    *sc_out1_clone.lock().unwrap() = Some(cc.new_subchannel(&addr1).0);
                    *sc_out2_clone.lock().unwrap() = Some(cc.new_subchannel(&addr2).0);
                })),
                ..Default::default()
            },
            opts,
        );

        let mut sharing = SubchannelSharing::new(mock, ws);

        sharing.work(None, &mut cc);

        // Verify that two new_subchannel calls occurred.
        let event1 = rx_events.recv().unwrap();
        let event2 = rx_events.recv().unwrap();
        assert!(matches!(event1, TestEvent::NewSubchannel(_)));
        assert!(matches!(event2, TestEvent::NewSubchannel(_)));

        assert!(rx_events.try_recv().is_err());

        // Verify that the two subchannels are different.
        let external_sc1 = sc_out1.lock().unwrap().take().unwrap();
        let external_sc2 = sc_out2.lock().unwrap().take().unwrap();

        assert!(!Arc::ptr_eq(&external_sc1, &external_sc2));
    }

    // Tests that when subchannels are dropped, they are removed from the
    // sharing map.
    #[test]
    fn test_subchannel_cleanup_on_drop() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let update_calls = Arc::new(Mutex::new(0));

        let sc_out1 = Arc::new(Mutex::new(None));
        let sc_out1_clone = sc_out1.clone();
        let sc_out2 = Arc::new(Mutex::new(None));
        let sc_out2_clone = sc_out2.clone();
        let sc_out3 = Arc::new(Mutex::new(None));
        let sc_out3_clone = sc_out3.clone();

        let work_calls = Arc::new(Mutex::new(0));
        let work_calls_clone = work_calls.clone();

        let ws = Arc::new(TestWorkScheduler::new(tx_events.clone()));
        let opts = LbPolicyOptions {
            work_scheduler: ws.clone(),
            runtime: default_runtime(),
        };
        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |_data, _workitem, cc| {
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    let mut num_calls = work_calls_clone.lock().unwrap();
                    if *num_calls == 0 {
                        *sc_out1_clone.lock().unwrap() = Some(cc.new_subchannel(&addr).0);
                        *sc_out2_clone.lock().unwrap() = Some(cc.new_subchannel(&addr).0);
                    } else if *num_calls == 1 {
                        *sc_out3_clone.lock().unwrap() = Some(cc.new_subchannel(&addr).0);
                    }
                    *num_calls += 1;
                })),
                ..Default::default()
            },
            opts,
        );

        let mut sharing = SubchannelSharing::new(mock, ws.clone() as Arc<dyn WorkScheduler>);

        // The first call to work should create sc1 and sc2.
        sharing.work(None, &mut cc);
        let _ = rx_events.recv().unwrap();

        let external_sc1 = sc_out1.lock().unwrap().take().unwrap();
        let external_sc2 = sc_out2.lock().unwrap().take().unwrap();

        let internal_sc = external_sc1.clone();

        // Subscribe to both external subchannels to verify they receive updates
        let calls1 = update_calls.clone();
        let _handle1 = external_sc1.subscribe(move |_state: SubchannelState| {
            *calls1.lock().unwrap() += 1;
        });
        let calls2 = update_calls.clone();
        let _handle2 = external_sc2.subscribe(move |_state: SubchannelState| {
            *calls2.lock().unwrap() += 1;
        });

        let state = SubchannelState::idle();
        let test_sc = internal_sc
            .downcast_ref::<crate::client::load_balancing::test_utils::TestSubchannel>()
            .unwrap();

        // Perform a subchannel update and confirm that two calls are made.
        test_sc.set_state(state.clone());
        assert_eq!(*update_calls.lock().unwrap(), 2);

        // Drop one external subchannel.
        drop(_handle1);
        drop(external_sc1);

        // Perform a subchannel update and confirm that only one call is made.
        *update_calls.lock().unwrap() = 0;
        test_sc.set_state(state.clone());
        assert_eq!(*update_calls.lock().unwrap(), 1);

        // We should have 2 strong references to the internal subchannel: ours,
        // and external_sc2 (maps only hold weak references).
        assert_eq!(Arc::strong_count(&internal_sc), 2);

        // Drop the other subchannel.
        drop(_handle2);
        drop(external_sc2);

        // Now there should be only our reference left to the internal
        // subchannel: ours.
        assert_eq!(Arc::strong_count(&internal_sc), 1);

        // Perform a subchannel update and confirm zero calls are made.
        *update_calls.lock().unwrap() = 0;
        test_sc.set_state(state.clone());
        assert_eq!(*update_calls.lock().unwrap(), 0);

        // Drop our local reference so that the internal subchannel is fully dropped.
        drop(internal_sc);

        // Drain any pending ScheduleWork events
        while rx_events.try_recv().is_ok() {}

        // Create a subchannel with the same address again and confirm that a
        // new underlying subchannel is created.
        sharing.work(None, &mut cc);
        let event = rx_events.recv().unwrap();
        let TestEvent::NewSubchannel(new_internal_sc) = event else {
            panic!("expected NewSubchannel")
        };

        let external_sc3 = sc_out3.lock().unwrap().take().unwrap();

        // Confirm a new subchannel was created.
        assert!(Arc::ptr_eq(&external_sc3, &new_internal_sc));
    }

    // Tests that single subchannel updates are sent to the delegate for every
    // duplicated shared subchannel.
    #[test]
    fn test_subchannel_update_broadcasts() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let update_calls = Arc::new(Mutex::new(0));

        let sc_out1 = Arc::new(Mutex::new(None));
        let sc_out1_clone = sc_out1.clone();
        let sc_out2 = Arc::new(Mutex::new(None));
        let sc_out2_clone = sc_out2.clone();

        let opts = test_lb_policy_options(tx_events.clone());
        let ws = opts.work_scheduler.clone();
        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |_data, _workitem, cc| {
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    *sc_out1_clone.lock().unwrap() = Some(cc.new_subchannel(&addr).0);
                    *sc_out2_clone.lock().unwrap() = Some(cc.new_subchannel(&addr).0);
                })),
                ..Default::default()
            },
            opts,
        );

        let mut sharing = SubchannelSharing::new(mock, ws);

        sharing.work(None, &mut cc);
        let _ = rx_events.recv().unwrap();

        let external_sc1 = sc_out1.lock().unwrap().take().unwrap();
        let external_sc2 = sc_out2.lock().unwrap().take().unwrap();

        let internal_sc = external_sc1.clone();

        let calls1 = update_calls.clone();
        let _handle1 = external_sc1.subscribe(move |_state: SubchannelState| {
            *calls1.lock().unwrap() += 1;
        });
        let calls2 = update_calls.clone();
        let _handle2 = external_sc2.subscribe(move |_state: SubchannelState| {
            *calls2.lock().unwrap() += 1;
        });

        let state = SubchannelState::idle();
        let test_sc = internal_sc
            .downcast_ref::<crate::client::load_balancing::test_utils::TestSubchannel>()
            .unwrap();

        // Verify that two delegated update calls are made.
        test_sc.set_state(state.clone());
        assert_eq!(*update_calls.lock().unwrap(), 2);

        // Drop one and verify that one delegated update call is made.
        drop(_handle1);
        drop(external_sc1);
        test_sc.set_state(state.clone());
        assert_eq!(*update_calls.lock().unwrap(), 3);
    }

    // Tests that the picker returns the shared subchannel.
    #[test]
    fn test_picker_returns_shared_subchannel() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let sc_out = Arc::new(Mutex::new(None));
        let sc_out_clone = sc_out.clone();

        let opts = test_lb_policy_options(tx_events.clone());
        let ws = opts.work_scheduler.clone();
        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |_data, _workitem, cc| {
                    let addr = Address {
                        address: "127.0.0.1:80".to_string().into(),
                        ..Default::default()
                    };
                    let sc = cc.new_subchannel(&addr).0;
                    *sc_out_clone.lock().unwrap() = Some(sc.clone());

                    #[derive(Debug)]
                    struct MockPicker {
                        sc: Arc<dyn Subchannel>,
                    }
                    impl Picker for MockPicker {
                        fn pick(&self, _req: &RequestHeaders) -> PickResult {
                            PickResult::Pick(Pick {
                                subchannel: self.sc.clone(),
                                metadata: MetadataMap::new(),
                                on_complete: None,
                            })
                        }
                    }

                    cc.update_picker(LbState {
                        connectivity_state: ConnectivityState::Ready,
                        picker: Arc::new(MockPicker { sc }),
                    });
                })),
                ..Default::default()
            },
            opts,
        );

        let mut sharing = SubchannelSharing::new(mock, ws);

        sharing.work(None, &mut cc);
        let _ = rx_events.recv().unwrap();

        let event = rx_events.recv().unwrap();
        let TestEvent::UpdatePicker(state) = event else {
            panic!("expected UpdatePicker")
        };

        let req = new_request_headers();
        let result = state.picker.pick(&req);
        let PickResult::Pick(pick) = result else {
            panic!("expected Pick")
        };

        let external_sc = sc_out.lock().unwrap().take().unwrap();

        assert!(Arc::ptr_eq(&pick.subchannel, &external_sc));
    }

    // Tests that update/work/exit_idle methods are delegated appropriately and
    // resolve_now is delegated back to the channel.
    #[test]
    fn test_delegates_other_methods() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };

        let called = Arc::new(Mutex::new(vec![]));

        let opts = test_lb_policy_options(tx_events.clone());
        let ws = opts.work_scheduler.clone();
        let mock = StubPolicy::new(
            StubPolicyFuncs {
                resolver_update: Some(Arc::new({
                    let called_clone = called.clone();
                    move |_data, _update, _config, _cc| {
                        called_clone.lock().unwrap().push("resolver_update");
                        Ok(())
                    }
                })),
                work: Some(Arc::new({
                    let called_clone = called.clone();
                    move |_data, _workitem, cc| {
                        called_clone.lock().unwrap().push("work");
                        cc.request_resolution();
                    }
                })),
                exit_idle: Some(Arc::new({
                    let called_clone = called.clone();
                    move |_data, _cc| called_clone.lock().unwrap().push("exit_idle")
                })),
            },
            opts,
        );

        let mut sharing = SubchannelSharing::new(mock, ws);

        let update = ResolverUpdate::default();
        sharing.resolver_update(update, None, &mut cc).unwrap();
        sharing.work(None, &mut cc);
        sharing.exit_idle(&mut cc);

        assert_eq!(
            *called.lock().unwrap(),
            vec!["resolver_update", "work", "exit_idle"]
        );

        let event = rx_events.recv().unwrap();
        assert!(matches!(event, TestEvent::RequestResolution));
    }

    // Tests that a shared subchannel's correct state is returned by
    // new_subchannel.
    #[test]
    fn test_new_subchannel_state() {
        let (tx_events, rx_events) = mpsc::channel();
        let mut cc = TestChannelController {
            tx_events: tx_events.clone(),
        };
        let (tx_work, rx_work) =
            mpsc::channel::<Box<dyn FnOnce(&mut dyn ChannelController) + Send>>();
        // Wrap rx_work in a mutex to allow the stub work Fn() closure to access
        // it mutably.
        let rx_work = Mutex::new(rx_work);

        let ws = Arc::new(TestWorkScheduler::new(tx_events.clone()));
        let opts = LbPolicyOptions {
            work_scheduler: ws.clone(),
            runtime: default_runtime(),
        };
        let mock = StubPolicy::new(
            StubPolicyFuncs {
                work: Some(Arc::new(move |_data, _workitem, cc| {
                    (rx_work.lock().unwrap().recv().unwrap())(cc);
                })),
                ..Default::default()
            },
            opts,
        );

        let mut sharing = SubchannelSharing::new(mock, ws.clone() as Arc<dyn WorkScheduler>);

        let addr = Address {
            address: "127.0.0.2:80".to_string().into(),
            ..Default::default()
        };

        let sc1 = Arc::new(Mutex::new(None));

        // Create the first subchannel
        let sc1_clone = sc1.clone();
        let addr_clone = addr.clone();
        tx_work
            .send(Box::new(move |cc| {
                let (sc, state) = cc.new_subchannel(&addr_clone);
                assert_eq!(state.connectivity_state, ConnectivityState::Idle);
                *sc1_clone.lock().unwrap() = Some(sc);
            }))
            .unwrap();
        sharing.work(None, &mut cc);

        let event = rx_events.recv().unwrap();
        let TestEvent::NewSubchannel(int_sc) = event else {
            panic!("expected NewSubchannel")
        };

        let test_sc = int_sc
            .downcast_ref::<crate::client::load_balancing::test_utils::TestSubchannel>()
            .unwrap();

        // Update the state to Connecting.
        test_sc.set_state(SubchannelState::connecting());
        crate::client::load_balancing::test_utils::run_pending_work(&ws, &mut sharing, &mut cc);

        // Create a second subchannel for the address and verify that the state
        // is also Connecting.
        let addr_clone = addr.clone();
        tx_work
            .send(Box::new(move |cc| {
                let (_sc, state) = cc.new_subchannel(&addr_clone);
                assert_eq!(state.connectivity_state, ConnectivityState::Connecting);
            }))
            .unwrap();
        sharing.work(None, &mut cc);

        // Update the state to Ready.
        test_sc.set_state(SubchannelState::ready());
        crate::client::load_balancing::test_utils::run_pending_work(&ws, &mut sharing, &mut cc);

        // Create another subchannel for the address and verify that the state
        // is now Ready.
        let addr_clone = addr.clone();
        tx_work
            .send(Box::new(move |cc| {
                let (_sc, state) = cc.new_subchannel(&addr_clone);
                assert_eq!(state.connectivity_state, ConnectivityState::Ready);
            }))
            .unwrap();
        sharing.work(None, &mut cc);
    }
}
