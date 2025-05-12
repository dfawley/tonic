#[cfg(test)]
mod tests {
    use crate::client::{
        load_balancing::{
            pick_first::{self, PickFirstConfig},
            test_utils::{
                self,
                test_utils::{FakeChannel, TestEvent, TestNopSubchannelImpl, TestWorkScheduler},
            },
            ChannelController, ErroringPicker, LbConfig, LbPolicyBuilder, LbPolicyOptions, LbState,
            ParsedJsonLbConfig, PickResult, QueuingPicker, Subchannel, SubchannelImpl,
            SubchannelState, WorkScheduler, GLOBAL_LB_REGISTRY,
        },
        name_resolution::{Address, Endpoint, ResolverData, ResolverUpdate},
        subchannel::{ConnectivityStateWatcher, InternalSubchannel, InternalSubchannelPool},
        transport::{Transport, GLOBAL_TRANSPORT_REGISTRY},
        ConnectivityState,
    };
    use crate::service::{Message, Request, Response, Service};
    use serde_json::json;
    use std::{ops::Add, sync::Arc};
    use tokio::{
        sync::{mpsc, Notify},
        task::AbortHandle,
    };
    use tonic::async_trait;

    #[test]
    fn pickfirst_builder_name() -> Result<(), String> {
        pick_first::reg();

        let builder: Arc<dyn LbPolicyBuilder> = match GLOBAL_LB_REGISTRY.get_policy("pick_first") {
            Some(b) => b,
            None => {
                return Err(String::from("pick_first LB policy not registered"));
            }
        };
        assert_eq!(builder.name(), "pick_first");
        Ok(())
    }

    #[test]
    fn pickfirst_builder_parse_config_failure() -> Result<(), String> {
        pick_first::reg();

        let builder: Arc<dyn LbPolicyBuilder> = match GLOBAL_LB_REGISTRY.get_policy("pick_first") {
            Some(b) => b,
            None => {
                return Err(String::from("pick_first LB policy not registered"));
            }
        };

        // Success cases.
        struct TestCase {
            config: ParsedJsonLbConfig,
            want_shuffle_addresses: Option<bool>,
        }
        let test_cases = vec![
            TestCase {
                config: ParsedJsonLbConfig(json!({})),
                want_shuffle_addresses: None,
            },
            TestCase {
                config: ParsedJsonLbConfig(json!({"shuffleAddressList": false})),
                want_shuffle_addresses: Some(false),
            },
            TestCase {
                config: ParsedJsonLbConfig(json!({"shuffleAddressList": true})),
                want_shuffle_addresses: Some(true),
            },
            TestCase {
                config: ParsedJsonLbConfig(
                    json!({"shuffleAddressList": true, "unknownField": "foo"}),
                ),
                want_shuffle_addresses: Some(true),
            },
        ];
        for tc in test_cases {
            let config = match builder.parse_config(&tc.config) {
                Ok(c) => c,
                Err(e) => {
                    let err = format!(
                        "parse_config({:?}) failed when expected to succeed: {:?}",
                        tc.config, e
                    )
                    .clone();
                    panic!("{}", err);
                }
            };
            let config: LbConfig = match config {
                Some(c) => c,
                None => {
                    let err = format!(
                        "parse_config({:?}) returned None when expected to succeed",
                        tc.config
                    )
                    .clone();
                    panic!("{}", err);
                }
            };
            let got_config: Arc<PickFirstConfig> = config.convert_to().unwrap();
            assert!(got_config.shuffle_address_list == tc.want_shuffle_addresses);
        }
        Ok(())
    }

    #[tokio::test]
    async fn pickfirst_connects_to_first_address() {
        // Setup the test environment.
        pick_first::reg();
        let (tx_events, mut rx_events) = mpsc::unbounded_channel::<TestEvent>();
        let work_scheduler = Arc::new(TestWorkScheduler {
            tx_events: tx_events.clone(),
        });
        let mut tcc = FakeChannel {
            tx_events: tx_events.clone(),
        };

        // Build the pick_first LB policy.
        let builder: Arc<dyn LbPolicyBuilder> =
            GLOBAL_LB_REGISTRY.get_policy("pick_first").unwrap();
        let mut lb_policy = builder.build(LbPolicyOptions { work_scheduler });

        // Send a resolver update with two addresses.
        const ADDRESS_1: &str = "1.1.1.1:1111";
        const ADDRESS_2: &str = "2.2.2.2:2222";
        let address1 = Address {
            address: String::from(ADDRESS_1),
            ..Default::default()
        };
        let address2 = Address {
            address: String::from(ADDRESS_2),
            ..Default::default()
        };
        let update = ResolverUpdate::Data(ResolverData {
            endpoints: vec![Endpoint {
                addresses: vec![address1.clone(), address2.clone()],
                ..Default::default()
            }],
            ..Default::default()
        });
        assert!(lb_policy.resolver_update(update, None, &mut tcc).is_ok());

        // Verify that subchannels are created for the above addresses.
        let sc1 = match rx_events.recv().await.unwrap() {
            TestEvent::NewSubchannel(addr, sc) => {
                assert!(addr == address1);
                sc
            }
            _ => panic!("unexpected event"),
        };
        let sc2 = match rx_events.recv().await.unwrap() {
            TestEvent::NewSubchannel(addr, sc) => {
                assert!(addr == address2);
                sc
            }
            _ => panic!("unexpected event"),
        };

        // Send initial state of IDLE for the above subchannels.
        lb_policy.subchannel_update(sc1.clone(), &SubchannelState::default(), &mut tcc);
        lb_policy.subchannel_update(sc2.clone(), &SubchannelState::default(), &mut tcc);

        // Ensure that a connection is attempted to the first subchannel.
        match rx_events.recv().await.unwrap() {
            TestEvent::Connect(addr) => {
                assert!(addr == address1);
            }
            _ => panic!("unexpected event"),
        };

        // Move first subchannel to CONNECTING.
        lb_policy.subchannel_update(
            sc1.clone(),
            &SubchannelState {
                connectivity_state: ConnectivityState::Connecting,
                ..Default::default()
            },
            &mut tcc,
        );

        // Ensure that the channel becomes CONNECTING, with a queuing picker.
        let picker = match rx_events.recv().await.unwrap() {
            TestEvent::UpdatePicker(update) => {
                assert!(update.connectivity_state == ConnectivityState::Connecting);
                update.picker
            }
            _ => panic!("unexpected event"),
        };
        let req = test_utils::test_utils::new_request();
        assert!(picker.pick(&req) == PickResult::Queue);

        // Move first subchannel to READY.
        lb_policy.subchannel_update(
            sc1.clone(),
            &SubchannelState {
                connectivity_state: ConnectivityState::Ready,
                ..Default::default()
            },
            &mut tcc,
        );

        // Ensure that the channel becomes READY, with a ready picker.
        let picker = match rx_events.recv().await.unwrap() {
            TestEvent::UpdatePicker(update) => {
                assert!(update.connectivity_state == ConnectivityState::Ready);
                update.picker
            }
            _ => panic!("unexpected event"),
        };
        let req = test_utils::test_utils::new_request();
        match picker.pick(&req) {
            PickResult::Pick(pick) => {
                assert!(pick.subchannel == sc1.clone());
            }
            _ => panic!("unexpected pick result"),
        }

        // Move first subchannel to Idle.
        lb_policy.subchannel_update(
            sc1.clone(),
            &SubchannelState {
                connectivity_state: ConnectivityState::Idle,
                ..Default::default()
            },
            &mut tcc,
        );

        // Ensure that the channel moves to IDLE.
        match rx_events.recv().await.unwrap() {
            TestEvent::UpdatePicker(update) => {
                assert!(update.connectivity_state == ConnectivityState::Idle);
            }
            _ => panic!("unexpected event"),
        };
    }
}
