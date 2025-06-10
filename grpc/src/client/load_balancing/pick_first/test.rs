use crate::client::{
    load_balancing::{
        pick_first::{
            self, thread_rng_shuffler, EndpointShuffler, PickFirstConfig, SHUFFLE_ENDPOINTS_FN,
        },
        test_utils::{self, FakeChannel, TestEvent, TestWorkScheduler},
        ChannelController, ExternalSubchannel, Failing, LbConfig, LbPolicy, LbPolicyBuilder,
        LbPolicyOptions, LbState, ParsedJsonLbConfig, PickResult, Picker, QueuingPicker,
        Subchannel, SubchannelState, WorkScheduler, GLOBAL_LB_REGISTRY,
    },
    name_resolution::{Address, Endpoint, ResolverUpdate},
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

fn create_endpoint_with_one_address(addr: String) -> Endpoint {
    Endpoint {
        addresses: vec![Address {
            address: addr,
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
    let update = ResolverUpdate {
        endpoints: Ok(endpoints.clone()),
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
    let update = ResolverUpdate {
        endpoints: Ok(endpoints.clone()),
        ..Default::default()
    };

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
            other => panic!("unexpected event {}", other),
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
    //calling lb_policy subchannel update
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
    err: &str,
    tcc: &mut dyn ChannelController,
) {
    lb_policy.subchannel_update(
        subchannel.clone(),
        &SubchannelState {
            connectivity_state: ConnectivityState::TransientFailure,
            last_connection_error: Some(Arc::from(Box::from(err.to_owned()))),
        },
        tcc,
    );
}

// Verifies that a connection attempt is made to the given subchannel.
async fn verify_connection_attempt_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    subchannel: Arc<dyn Subchannel>,
) {
    let event = rx_events.recv().await.unwrap();
    println!("  Subchannel: {}", event);
    match event {
    // match rx_events.recv().await.unwrap() {
        TestEvent::Connect(addr) => {
            assert!(addr == subchannel.address());
        }
        other => panic!("unexpected event {}", other),
    };
}

// Verifies that a call to schedule_work is made by the LB policy.
async fn verify_schedule_work_from_policy(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
    match rx_events.recv().await.unwrap() {
        TestEvent::ScheduleWork => {}
        other => panic!("unexpected event {}", other),
    };
}

// Verifies that the channel moves to IDLE state.
//
// Returns the picker for tests to make more picks, if required.
async fn verify_idle_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
) -> Arc<dyn Picker> {
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            assert!(update.connectivity_state == ConnectivityState::Idle);
            update.picker.clone()
        }
        other => panic!("unexpected event {}", other),
    }
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
        other => panic!("unexpected event {}", other),
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
                other => panic!("unexpected pick result {}", other),
            }
        }
        other => panic!("unexpected event {}", other),
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
                other => panic!("unexpected pick result {}", other),
            }
        }
        other => panic!("unexpected event {}", other),
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
        other => panic!("unexpected event {}", other),
    };
}

// Verifies that the LB policy requests re-resolution.
async fn verify_resolution_request(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
    match rx_events.recv().await.unwrap() {
        TestEvent::RequestResolution => {}
        other => panic!("unexpected event {}", other),
    };
}

const DEFAULT_TEST_SHORT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

async fn verify_no_activity_from_policy(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
    tokio::select! {
        _ = tokio::time::sleep(DEFAULT_TEST_SHORT_TIMEOUT) => {}
        event = rx_events.recv() => {
            panic!("unexpected event {}", event.unwrap());
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
        other => panic!("unexpected pick result {}", other),
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
        other => panic!("unexpected pick result {}", other),
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

    let endpoint1 = create_endpoint_with_one_address("1.1.1.1:1".to_string());
    let endpoint2 = create_endpoint_with_one_address("2.2.2.2:2".to_string());
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

// Tests the scenario where the resolver returns an update with multiple
// addresses and the LB policy successfully connects to first one and moves to
// READY. The resolver then returns an update with a new address list that does
// not contain the address of the currently connected subchannel. The LB policy
// should create subchannels for the new addresses, and then realize that the
// currently connected subchannel is not in the new address list. It should then
// move to IDLE state and return a picker that queues RPCs. When an RPC is made,
// the LB policy should create subchannels for the addresses specified in the
// previous update and start connecting to them.
#[tokio::test]
async fn pickfirst_resolver_update_with_completely_new_address_list() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    let endpoints = create_endpoint_with_one_address("3.3.3.3:3".to_string());
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    let picker = verify_idle_picker_from_policy(&mut rx_events).await;
    verify_resolution_request(&mut rx_events).await;
    let req = test_utils::new_request();
    assert!(picker.pick(&req) == PickResult::Queue);
    verify_schedule_work_from_policy(&mut rx_events).await;
    lb_policy.work(tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
}

// Tests the scenario where the resolver returns an update with multiple
// addresses and the LB policy successfully connects to first one and moves to
// READY. The resolver then returns an update with a new address list that
// contains the address of the currently connected subchannel. The LB policy
// should create subchannels for the new addresses, and then see that the
// currently connected subchannel is in the new address list. It should then
// send a new READY picker that returns the currently connected subchannel.
#[tokio::test]
async fn pickfirst_resolver_update_contains_currently_ready_subchannel() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    let mut endpoints = create_endpoint_with_n_addresses(4);
    endpoints.addresses.reverse();
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    lb_policy.subchannel_update(subchannels[0].clone(), &SubchannelState::default(), tcc);
    lb_policy.subchannel_update(subchannels[1].clone(), &SubchannelState::default(), tcc);
    lb_policy.subchannel_update(subchannels[2].clone(), &SubchannelState::default(), tcc);
    lb_policy.subchannel_update(
        subchannels[3].clone(),
        &SubchannelState {
            connectivity_state: ConnectivityState::Ready,
            ..Default::default()
        },
        tcc,
    );
    verify_ready_picker_from_policy(&mut rx_events, subchannels[3].clone()).await;
}

// Tests the scenario where the resolver returns an update with multiple
// addresses and the LB policy successfully connects to first one and moves to
// READY. The resolver then returns an update with a an address list that is
// identical to the first update. The LB policy should create subchannels for
// the new addresses, and then see that the currently connected subchannel is in
// the new address list. It should then send a new READY picker that returns the
// currently connected subchannel.
#[tokio::test]
async fn pickfirst_resolver_update_contains_identical_address_list() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    lb_policy.subchannel_update(
        subchannels[0].clone(),
        &SubchannelState {
            connectivity_state: ConnectivityState::Ready,
            ..Default::default()
        },
        tcc,
    );
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
}

// Tests the scenario where the resolver returns an update with multiple
// addresses and the LB policy successfully connects to first one and moves to
// READY. The resolver then returns an update with a new address list that
// removes the address of the currently connected subchannel. The LB policy
// should create subchannels for the new addresses, and then see that the
// currently connected subchannel is not in the new address list. It should then
// move to IDLE state and return a picker that queues RPCs. When an RPC is made,
// the LB policy should create subchannels for the addresses specified in the
// previous update and start connecting to them. The test repeats this scenario
// multiple times, each time removing the first address from the address list,
// eventually ending up with an empty address list. The LB policy should move to
// TRANSIENT_FAILURE state with a failing picker.
#[tokio::test]
async fn pickfirst_resolver_update_removes_connected_address() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let mut endpoints = create_endpoint_with_n_addresses(3);
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    // Address list now contains two addresses.
    endpoints.addresses.remove(0);
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    let picker = verify_idle_picker_from_policy(&mut rx_events).await;
    verify_resolution_request(&mut rx_events).await;
    let req = test_utils::new_request();
    assert!(picker.pick(&req) == PickResult::Queue);
    verify_schedule_work_from_policy(&mut rx_events).await;
    lb_policy.work(tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    // Address list now contains one address.
    endpoints.addresses.remove(0);
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    let picker = verify_idle_picker_from_policy(&mut rx_events).await;
    verify_resolution_request(&mut rx_events).await;
    let req = test_utils::new_request();
    assert!(picker.pick(&req) == PickResult::Queue);
    verify_schedule_work_from_policy(&mut rx_events).await;
    lb_policy.work(tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    // Address list is now empty.
    endpoints.addresses.remove(0);
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoints]),
        ..Default::default()
    };
    assert!(lb_policy.resolver_update(update, None, tcc).is_err());
    verify_transient_failure_picker_from_policy(
        &mut rx_events,
        "received empty address list from the name resolver".to_string(),
    )
    .await;
}

// Tests the scenario where the resolver returns an update with multiple
// addresses and the LB policy successfully connects to first one and moves to
// READY. The connected subchannel then goes down and the LB policy moves to IDLE
// state with a picker that queues RPCs. When an RPC is made, the LB policy
// creates subchannels for the addresses specified in the previous update and
// starts connecting to them. The LB policy should then move to READY state with
// a picker that returns the second subchannel.
#[tokio::test]
async fn pickfirst_connected_subchannel_goes_down() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_idle(lb_policy, subchannels[0].clone(), tcc);
    let picker = verify_idle_picker_from_policy(&mut rx_events).await;
    verify_resolution_request(&mut rx_events).await;
    let req = test_utils::new_request();
    assert!(picker.pick(&req) == PickResult::Queue);
    verify_schedule_work_from_policy(&mut rx_events).await;
    lb_policy.work(tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_transient_failure(
        lb_policy,
        subchannels[0].clone(),
        "connection error",
        tcc,
    );
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    move_subchannel_to_ready(lb_policy, subchannels[1].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[1].clone()).await;
}

// Tests the scenario where the resolver returns an update with multiple
// addresses and the LB policy successfully connects to first one and moves to
// READY. The connected subchannel then goes down and the LB policy moves to IDLE
// state with a picker that queues RPCs. When an RPC is made, the LB policy
// creates subchannels for the addresses specified in the previous update and
// starts connecting to them. All subchannels fail to connect and the LB policy
// moves to TRANSIENT_FAILURE state with a failing picker.
#[tokio::test]
async fn pickfirst_all_subchannels_goes_down() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoints.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_idle(lb_policy, subchannels[0].clone(), tcc);
    let picker = verify_idle_picker_from_policy(&mut rx_events).await;
    verify_resolution_request(&mut rx_events).await;
    let req = test_utils::new_request();
    assert!(picker.pick(&req) == PickResult::Queue);
    verify_schedule_work_from_policy(&mut rx_events).await;
    lb_policy.work(tcc);

    let connection_error = String::from("test connection error 2");
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_transient_failure(lb_policy, subchannels[0].clone(), &connection_error, tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[1].clone(), tcc);
    move_subchannel_to_transient_failure(lb_policy, subchannels[1].clone(), &connection_error, tcc);
    verify_transient_failure_picker_from_policy(&mut rx_events, connection_error).await;
}
