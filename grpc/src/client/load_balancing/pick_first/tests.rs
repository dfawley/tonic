use crate::client::{
    load_balancing::{
        pick_first::{self, PickFirstConfig},
        test_utils::{self, FakeChannel, TestEvent, TestNopSubchannelImpl, TestWorkScheduler},
        ChannelController, ErroringPicker, LbConfig, LbPolicy, LbPolicyBuilder, LbPolicyOptions,
        LbState, ParsedJsonLbConfig, PickResult, QueuingPicker, Subchannel, SubchannelImpl,
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
            config: ParsedJsonLbConfig(json!({"shuffleAddressList": true, "unknownField": "foo"})),
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

// Sets up the test environment.
//
// Performs the following:
// 1. Creates a work scheduler.
// 2. Creates a fake channel that acts as a channel controller.
// 3. Creates a pick_first LB policy.
//
// Returns the following:
// 1. A receiver for events initiated by the LB policy (like creating a
//    new subchannel, sending a new picker etc).
// 2. The LB policy to send resolver and subchannel updates from the test.
// 3. The channel to pass to the LB policy as part of the updates.
fn setup() -> (
    mpsc::UnboundedReceiver<TestEvent>,
    Box<dyn LbPolicy>,
    Box<dyn ChannelController>,
) {
    pick_first::reg();
    let (tx_events, rx_events) = mpsc::unbounded_channel::<TestEvent>();
    let work_scheduler = Arc::new(TestWorkScheduler {
        tx_events: tx_events.clone(),
    });
    let tcc = Box::new(FakeChannel {
        tx_events: tx_events.clone(),
    });
    let builder: Arc<dyn LbPolicyBuilder> = GLOBAL_LB_REGISTRY.get_policy("pick_first").unwrap();
    let lb_policy = builder.build(LbPolicyOptions { work_scheduler });

    (rx_events, lb_policy, tcc)
}

// Creates a new endpoint with the specified number of addresses.
fn create_endpoint_with_n_addresses(n: usize) -> Endpoint {
    let mut addresses = Vec::new();
    for i in 0..n {
        addresses.push(Address {
            address: format!("{}.{}.{}.{}:{}", i, i, i, i, i),
            ..Default::default()
        });
    }
    Endpoint {
        addresses,
        ..Default::default()
    }
}

// Sends a resolver update to the LB policy with the specified endpoint.
fn send_resolver_update_to_policy(
    lb_policy: &mut dyn LbPolicy,
    endpoint: Endpoint,
    tcc: &mut dyn ChannelController,
) {
    let update = ResolverUpdate::Data(ResolverData {
        endpoints: vec![endpoint],
        ..Default::default()
    });
    assert!(lb_policy.resolver_update(update, None, tcc).is_ok());
}

// Verifies that the subchannels are created for the given addresses in the
// given order. Returns the subchannels created.
async fn verify_subchannel_creation_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    addresses: Vec<Address>,
) -> Vec<Arc<dyn Subchannel>> {
    let mut subchannels = Vec::new();
    for address in addresses {
        match rx_events.recv().await.unwrap() {
            TestEvent::NewSubchannel(addr, sc) => {
                assert!(addr == address.clone());
                subchannels.push(sc);
            }
            _ => panic!("unexpected event"),
        };
    }
    subchannels
}

// Sends initial subchannel updates to the LB policy for the given
// subchannels, with their state set to IDLE.
fn send_initial_subchannel_updates_to_policy(
    lb_policy: &mut dyn LbPolicy,
    subchannels: &[Arc<dyn Subchannel>],
    tcc: &mut dyn ChannelController,
) {
    for sc in subchannels {
        lb_policy.subchannel_update(sc.clone(), &SubchannelState::default(), tcc);
    }
}

fn move_subchannel_to_connecting(
    lb_policy: &mut dyn LbPolicy,
    subchannel: Arc<dyn Subchannel>,
    tcc: &mut dyn ChannelController,
) {
    lb_policy.subchannel_update(
        subchannel.clone(),
        &SubchannelState {
            connectivity_state: ConnectivityState::Connecting,
            ..Default::default()
        },
        tcc,
    );
}

fn move_subchannel_to_ready(
    lb_policy: &mut dyn LbPolicy,
    subchannel: Arc<dyn Subchannel>,
    tcc: &mut dyn ChannelController,
) {
    lb_policy.subchannel_update(
        subchannel.clone(),
        &SubchannelState {
            connectivity_state: ConnectivityState::Ready,
            ..Default::default()
        },
        tcc,
    );
}

fn move_subchannel_to_idle(
    lb_policy: &mut dyn LbPolicy,
    subchannel: Arc<dyn Subchannel>,
    tcc: &mut dyn ChannelController,
) {
    lb_policy.subchannel_update(
        subchannel.clone(),
        &SubchannelState {
            connectivity_state: ConnectivityState::Idle,
            ..Default::default()
        },
        tcc,
    );
}

// Verifies that a connection attempt is made to the given subchannel.
async fn verify_connection_attempt_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    subchannel: Arc<dyn Subchannel>,
) {
    match rx_events.recv().await.unwrap() {
        TestEvent::Connect(addr) => {
            assert!(addr == *subchannel.address());
        }
        _ => panic!("unexpected event"),
    };
}

// Verifies that the channel moves to CONNECTING state with a queuing
// picker.
async fn verify_connecting_picker_from_policy(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            assert!(update.connectivity_state == ConnectivityState::Connecting);
            let req = test_utils::new_request();
            assert!(update.picker.pick(&req) == PickResult::Queue);
        }
        _ => panic!("unexpected event"),
    };
}

// Verifies that the channel moves to READY state with a ready picker.
async fn verify_ready_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    subchannel: Arc<dyn Subchannel>,
) {
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            assert!(update.connectivity_state == ConnectivityState::Ready);
            let req = test_utils::new_request();
            match update.picker.pick(&req) {
                PickResult::Pick(pick) => {
                    assert!(pick.subchannel == subchannel.clone());
                }
                _ => panic!("unexpected pick result"),
            }
        }
        _ => panic!("unexpected event"),
    };
}

// Verifies that the channel moves to IDLE state.
async fn verify_channel_moves_to_idle(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            assert!(update.connectivity_state == ConnectivityState::Idle);
        }
        _ => panic!("unexpected event"),
    };
}

#[tokio::test]
async fn pickfirst_connects_to_first_address() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    // Send a resolver update with two addresses, and verify that
    // subchannels are created for the those addresses.
    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, endpoint.clone(), tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);

    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_idle(lb_policy, subchannels[0].clone(), tcc);

    verify_channel_moves_to_idle(&mut rx_events).await;
}
