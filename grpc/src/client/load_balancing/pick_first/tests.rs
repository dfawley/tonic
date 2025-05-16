use crate::client::{
    load_balancing::{
        pick_first::{
            self, thread_rng_shuffler, EndpointShuffler, PickFirstConfig, SHUFFLE_ENDPOINTS_FN,
        },
        test_utils::{self, FakeChannel, TestEvent, TestWorkScheduler},
        ChannelController, Failing, LbConfig, LbPolicy, LbPolicyBuilder, LbPolicyOptions, LbState,
        ParsedJsonLbConfig, PickResult, Picker, QueuingPicker, Subchannel, SubchannelImpl,
        SubchannelState, WorkScheduler, GLOBAL_LB_REGISTRY,
    },
    name_resolution::{Address, Endpoint, ResolverUpdate},
    subchannel::{InternalSubchannel, InternalSubchannelPool},
    transport::{Transport, GLOBAL_TRANSPORT_REGISTRY},
    ConnectivityState,
};
use crate::service::{Message, Request, Response, Service};
use core::panic;
use rand::{rngs::StdRng, Rng, RngCore, SeedableRng};
use serde_json::json;
use std::{
    ops::Add,
    sync::{Arc, Mutex},
};
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

fn create_endpoint_with_one_address(addr: usize) -> Endpoint {
    Endpoint {
        addresses: vec![Address {
            address: format!("{}.{}.{}.{}:{}", addr, addr, addr, addr, addr),
            ..Default::default()
        }],
        ..Default::default()
    }
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
    endpoints: Vec<Endpoint>,
    tcc: &mut dyn ChannelController,
) {
    let update = ResolverUpdate::Data(ResolverData {
        endpoints: endpoints.clone(),
        ..Default::default()
    };
    assert!(lb_policy.resolver_update(update, None, tcc).is_ok());
}

// Sends a resolver update with LB config enabling address shuffling to the LB
// policy with the specified endpoint.
fn send_resolver_update_with_lb_config_to_policy(
    lb_policy: &mut dyn LbPolicy,
    endpoints: Vec<Endpoint>,
    tcc: &mut dyn ChannelController,
) {
    let update = ResolverUpdate::Data(ResolverData {
        endpoints: endpoints.clone(),
        ..Default::default()
    });

    let json_config = ParsedJsonLbConfig(json!({"shuffleAddressList": true}));
    let builder = GLOBAL_LB_REGISTRY.get_policy("pick_first").unwrap();
    let config = builder.parse_config(&json_config).unwrap();

    assert!(lb_policy
        .resolver_update(update, config.as_ref(), tcc)
        .is_ok());
}

// Sends a resolver error to the LB policy with the specified error message.
fn send_resolver_error_to_policy(
    lb_policy: &mut dyn LbPolicy,
    err: String,
    tcc: &mut dyn ChannelController,
) {
    let update = ResolverUpdate {
        endpoints: Err(err),
        ..Default::default()
    };
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

fn move_subchannel_to_transient_failure(
    lb_policy: &mut dyn LbPolicy,
    subchannel: Arc<dyn Subchannel>,
    err: &String,
    tcc: &mut dyn ChannelController,
) {
    lb_policy.subchannel_update(
        subchannel.clone(),
        &SubchannelState {
            connectivity_state: ConnectivityState::TransientFailure,
            last_connection_error: Some(Arc::from(Box::from(err.clone()))),
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

// Verifies that the channel moves to CONNECTING state with a queuing picker.
//
// Returns the picker for tests to make more picks, if required.
async fn verify_connecting_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
) -> Arc<dyn Picker> {
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            assert!(update.connectivity_state == ConnectivityState::Connecting);
            let req = test_utils::new_request();
            assert!(update.picker.pick(&req) == PickResult::Queue);
            update.picker.clone()
        }
        _ => panic!("unexpected event"),
    }
}

// Verifies that the channel moves to READY state with a picker that returns the
// given subchannel.
//
// Returns the picker for tests to make more picks, if required.
async fn verify_ready_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    subchannel: Arc<dyn Subchannel>,
) -> Arc<dyn Picker> {
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            assert!(update.connectivity_state == ConnectivityState::Ready);
            let req = test_utils::new_request();
            match update.picker.pick(&req) {
                PickResult::Pick(pick) => {
                    assert!(pick.subchannel == subchannel.clone());
                    update.picker.clone()
                }
                _ => panic!("unexpected pick result"),
            }
        }
        _ => panic!("unexpected event"),
    }
}

// Verifies that the channel moves to TRANSIENT_FAILURE state with a picker
// that returns an error with the given message. The error code should be
// UNAVAILABLE..
//
// Returns the picker for tests to make more picks, if required.
async fn verify_transient_failure_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    want_error: String,
) -> Arc<dyn Picker> {
    let picker = match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            assert!(update.connectivity_state == ConnectivityState::TransientFailure);
            let req = test_utils::new_request();
            match update.picker.pick(&req) {
                PickResult::Fail(status) => {
                    assert!(status.code() == tonic::Code::Unavailable);
                    assert!(status.message().contains(&want_error));
                    update.picker.clone()
                }
                _ => panic!("unexpected pick result"),
            }
        }
        _ => panic!("unexpected event"),
    };
    match rx_events.recv().await.unwrap() {
        TestEvent::RequestResolution => {}
        _ => panic!("no re-resolution requested after moving to transient_failure"),
    }
    picker
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

const DEFAULT_TEST_SHORT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

async fn verify_no_activity_from_policy(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
    tokio::select! {
        _ = tokio::time::sleep(DEFAULT_TEST_SHORT_TIMEOUT) => {}
        event = rx_events.recv() => {
            panic!("unexpected test event");
        }
    }
}

// Tests the scenario where the resolver returns an error before a valid update.
// The LB policy should move to TRANSIENT_FAILURE state with a failing picker.
#[tokio::test]
async fn pickfirst_resolver_error_before_a_valid_update() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let resolver_error = String::from("resolver error");
    send_resolver_error_to_policy(lb_policy, resolver_error.clone(), tcc);
    verify_transient_failure_picker_from_policy(&mut rx_events, resolver_error).await;
}

// Tests the scenario where the resolver returns an error after a valid update
// and the LB policy has moved to READY. The LB policy should ignore the error
// and continue using the previously received update.
#[tokio::test]
async fn pickfirst_resolver_error_after_a_valid_update_in_ready() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);

    let picker = verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    let resolver_error = String::from("resolver error");
    send_resolver_error_to_policy(lb_policy, resolver_error.clone(), tcc);
    verify_no_activity_from_policy(&mut rx_events).await;

    let req = test_utils::new_request();
    match picker.pick(&req) {
        PickResult::Pick(pick) => {
            assert!(pick.subchannel == subchannels[0].clone());
        }
        _ => panic!("unexpected pick result"),
    }
}

// Tests the scenario where the resolver returns an error after a valid update
// and the LB policy is still trying to connect. The LB policy should ignore the
// error and continue using the previously received update.
#[tokio::test]
async fn pickfirst_resolver_error_after_a_valid_update_in_connecting() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    let picker = verify_connecting_picker_from_policy(&mut rx_events).await;

    let resolver_error = String::from("resolver error");
    send_resolver_error_to_policy(lb_policy, resolver_error.clone(), tcc);
    verify_no_activity_from_policy(&mut rx_events).await;

    let req = test_utils::new_request();
    match picker.pick(&req) {
        PickResult::Queue => {}
        _ => panic!("unexpected pick result"),
    }
}

// Tests the scenario where the resolver returns an error after a valid update
// and the LB policy has moved to TRANSIENT_FAILURE after attemting to connect
// to all addresses.  The LB policy should send a new picker that returns the
// error from the resolver.
#[tokio::test]
async fn pickfirst_resolver_error_after_a_valid_update_in_tf() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;

    let connection_error = String::from("test connection error");
    move_subchannel_to_transient_failure(lb_policy, subchannels[0].clone(), &connection_error, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[1].clone(), tcc);
    move_subchannel_to_transient_failure(lb_policy, subchannels[1].clone(), &connection_error, tcc);
    verify_transient_failure_picker_from_policy(&mut rx_events, connection_error).await;

    let resolver_error = String::from("resolver error");
    send_resolver_error_to_policy(lb_policy, resolver_error.clone(), tcc);
    verify_transient_failure_picker_from_policy(&mut rx_events, resolver_error).await;
}

// Tests the scenario where the resolver returns an update with no addresses
// (before sending any valid update). The LB policy should move to
// TRANSIENT_FAILURE state with a failing picker.
#[tokio::test]
async fn pickfirst_zero_addresses_from_resolver_before_valid_update() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(0);
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint]),
        ..Default::default()
    };
    assert!(lb_policy.resolver_update(update, None, tcc).is_err());
    verify_transient_failure_picker_from_policy(
        &mut rx_events,
        "received empty address list from the name resolver".to_string(),
    )
    .await;
}

// Tests the scenario where the resolver returns an update with no endpoints
// (before sending any valid update). The LB policy should move to
// TRANSIENT_FAILURE state with a failing picker.
#[tokio::test]
async fn pickfirst_zero_endpoints_from_resolver_before_valid_update() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let update = ResolverUpdate {
        endpoints: Ok(vec![]),
        ..Default::default()
    };
    assert!(lb_policy.resolver_update(update, None, tcc).is_err());
    verify_transient_failure_picker_from_policy(
        &mut rx_events,
        "received empty address list from the name resolver".to_string(),
    )
    .await;
}

// Tests the scenario where the resolver returns an update with no endpoints
// after sending a valid update (and the LB policy has moved to READY). The LB
// policy should move to TRANSIENT_FAILURE state with a failing picker.
#[tokio::test]
async fn pickfirst_zero_endpoints_from_resolver_after_valid_update() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);

    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    let update = ResolverUpdate {
        endpoints: Ok(vec![]),
        ..Default::default()
    };
    assert!(lb_policy.resolver_update(update, None, tcc).is_err());
    verify_transient_failure_picker_from_policy(
        &mut rx_events,
        "received empty address list from the name resolver".to_string(),
    )
    .await;
}

// Tests the scenario where the resolver returns an update with one address. The
// LB policy should create a subchannel for that address, connect to it, and
// once the connection succeeds, move to READY state with a picker that returns
// that subchannel.
#[tokio::test]
async fn pickfirst_with_one_backend() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(1);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);

    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
}

// Tests the scenario where the resolver returns an update with multiple
// address. The LB policy should create subchannels for all address, and attempt
// to connect to them in order, until a connection succeeds, at which point it
// should move to READY state with a picker that returns that subchannel.
#[tokio::test]
async fn pickfirst_with_multiple_backends_first_backend_is_ready() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);

    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
}

// Tests the scenario where the resolver returns an update with multiple
// address. The LB policy should create subchannels for all address, and attempt
// to connect to them in order, until a connection succeeds, at which point it
// should move to READY state with a picker that returns that subchannel.
#[tokio::test]
async fn pickfirst_with_multiple_backends_first_backend_is_not_ready() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(3);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    let connection_error = String::from("test connection error");
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_transient_failure(lb_policy, subchannels[0].clone(), &connection_error, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[1].clone(), tcc);
    move_subchannel_to_transient_failure(lb_policy, subchannels[1].clone(), &connection_error, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[2].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[2].clone(), tcc);
    move_subchannel_to_ready(lb_policy, subchannels[2].clone(), tcc);

    verify_ready_picker_from_policy(&mut rx_events, subchannels[2].clone()).await;
}

// Tests the scenario where the resolver returns an update with multiple
// address, some of which are duplicates. The LB policy should dedup the
// addresses and create subchannels for them, and attempt to connect to them in
// order, until a connection succeeds, at which point it should move to READY
// state with a picker that returns that subchannel.
#[tokio::test]
async fn pickfirst_with_multiple_backends_duplicate_addresses() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = Endpoint {
        addresses: vec![
            Address {
                address: format!("{}.{}.{}.{}:{}", 0, 0, 0, 0, 0),
                ..Default::default()
            },
            Address {
                address: format!("{}.{}.{}.{}:{}", 0, 0, 0, 0, 0),
                ..Default::default()
            },
            Address {
                address: format!("{}.{}.{}.{}:{}", 1, 1, 1, 1, 1),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let endpoint_with_duplicates_removed = Endpoint {
        addresses: vec![
            Address {
                address: format!("{}.{}.{}.{}:{}", 0, 0, 0, 0, 0),
                ..Default::default()
            },
            Address {
                address: format!("{}.{}.{}.{}:{}", 1, 1, 1, 1, 1),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels = verify_subchannel_creation_from_policy(
        &mut rx_events,
        endpoint_with_duplicates_removed.addresses.clone(),
    )
    .await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    let connection_error = String::from("test connection error");
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_transient_failure(lb_policy, subchannels[0].clone(), &connection_error, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[1].clone(), tcc);
    move_subchannel_to_ready(lb_policy, subchannels[1].clone(), tcc);

    verify_ready_picker_from_policy(&mut rx_events, subchannels[1].clone()).await;
}

// Tests the scenario where the resolver returns an update with multiple
// addresses and connections to all of them fail. The LB policy should move to
// TRANSIENT_FAILURE state with a failing picker. It should then attempt to connect
// to the addresses again, and when they fail again, it should send a new
// picker that returns the most recent error message.
#[tokio::test]
async fn pickfirst_sticky_transient_failure() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    let first_error = String::from("test connection error 1");
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_transient_failure(lb_policy, subchannels[0].clone(), &first_error, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[1].clone(), tcc);
    move_subchannel_to_transient_failure(lb_policy, subchannels[1].clone(), &first_error, tcc);
    verify_transient_failure_picker_from_policy(&mut rx_events, first_error).await;

    // The subchannels need to complete their backoff before moving to IDLE, at
    // which point the LB policy should attempt to connect to them again.
    move_subchannel_to_idle(lb_policy, subchannels[0].clone(), tcc);
    move_subchannel_to_idle(lb_policy, subchannels[1].clone(), tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;

    let second_error = String::from("test connection error 2");
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    move_subchannel_to_transient_failure(lb_policy, subchannels[0].clone(), &second_error, tcc);
    move_subchannel_to_connecting(lb_policy, subchannels[1].clone(), tcc);
    move_subchannel_to_transient_failure(lb_policy, subchannels[1].clone(), &second_error, tcc);
    verify_transient_failure_picker_from_policy(&mut rx_events, second_error).await;

    // The subchannels need to complete their backoff before moving to IDLE, at
    // which point the LB policy should attempt to connect to them again.
    move_subchannel_to_idle(lb_policy, subchannels[0].clone(), tcc);
    move_subchannel_to_idle(lb_policy, subchannels[1].clone(), tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
}

// Overrides the default shuffler function with a custom one that reverses the
// order of the endpoints.
fn test_reverse_shuffler() -> Box<EndpointShuffler> {
    Box::new(|endpoints: &mut [Endpoint]| {
        endpoints.reverse();
    })
}

// Resets the shuffler function to the default one after the test completes
struct ShufflerResetGuard {}
impl Drop for ShufflerResetGuard {
    fn drop(&mut self) {
        *SHUFFLE_ENDPOINTS_FN.lock().unwrap() = thread_rng_shuffler();
    }
}

// Tests the scenario where the resolver returns an update with multiple
// endpoints and LB config with shuffle addresses enabled. We override the
// shuffler functionality to reverse the order of the endpoints. The LB policy
// should create subchannels for all addresses, and attempt to connect to them
// in order, until a connection succeeds, at which point it should move to READY
// state with a picker that returns that subchannel.
#[tokio::test]
async fn pickfirst_with_multiple_backends_shuffle_addresses() {
    let _guard = ShufflerResetGuard {};
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();
    *SHUFFLE_ENDPOINTS_FN.lock().unwrap() = test_reverse_shuffler();

    let endpoint1 = create_endpoint_with_one_address(1);
    let endpoint2 = create_endpoint_with_one_address(2);
    send_resolver_update_with_lb_config_to_policy(
        lb_policy,
        vec![endpoint1.clone(), endpoint2.clone()],
        tcc,
    );

    let subchannels = verify_subchannel_creation_from_policy(
        &mut rx_events,
        endpoint2
            .addresses
            .clone()
            .into_iter()
            .chain(endpoint1.addresses.iter().cloned())
            .collect(),
    )
    .await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);

    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
}
