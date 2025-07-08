use crate::client::{
    load_balancing::{
        pick_first::{self},
        round_robin::{self},
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
    collections::HashSet,
    ops::Add,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{mpsc, Notify},
    task::AbortHandle,
};
use tonic::async_trait;

#[test]
fn roundrobin_builder_name() -> Result<(), String> {
    round_robin::reg();

    let builder: Arc<dyn LbPolicyBuilder> = match GLOBAL_LB_REGISTRY.get_policy("round_robin") {
        Some(b) => b,
        None => {
            return Err(String::from("round_robin LB policy not registered"));
        }
    };
    assert_eq!(builder.name(), "round_robin");
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

//this should be good
fn setup() -> (
    mpsc::UnboundedReceiver<TestEvent>,
    Box<dyn LbPolicy>,
    Box<dyn ChannelController>,
) {
    pick_first::reg();
    round_robin::reg();

    let (tx_events, rx_events) = mpsc::unbounded_channel::<TestEvent>();
    let work_scheduler = Arc::new(TestWorkScheduler {
        tx_events: tx_events.clone(),
    });
    // let tcc = Box::new(FakeChannel::new(tx_events.clone()));
    let tcc = Box::new(FakeChannel {
        tx_events: tx_events.clone(),
    });
    let builder: Arc<dyn LbPolicyBuilder> = GLOBAL_LB_REGISTRY.get_policy("round_robin").unwrap();
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

fn create_n_endpoints_with_n_addresses(n: usize) -> Vec<Endpoint> {
    let mut addresses = Vec::new();
    let mut endpoints = Vec::new();
    for i in 0..n {
        let mut n_addresses = Vec::new();
        for i in 0..n {
            n_addresses.push(Address {
                address: format!("{}.{}.{}.{}:{}", i, i, i, i, i),
                ..Default::default()
            });
        }
        addresses.push(n_addresses);
    }
    for i in 0..n {
        endpoints.push(Endpoint {
            addresses: addresses[i].clone(),
            ..Default::default()
        })
    }
    endpoints
}

fn create_n_endpoints_with_k_addresses(n: usize, k: usize) -> Vec<Endpoint> {
    let mut addresses = Vec::new();
    let mut endpoints = Vec::new();
    for i in 0..n {
        let mut n_addresses = Vec::new();
        for j in 0..k {
            n_addresses.push(Address {
                address: format!("{}.{}.{}.{}:{}", j + i, j + i, j + i, j + i, j + i),
                // address: format!("{}.{}.{}.{}:{}", j, j, j, j, j),
                ..Default::default()
            });
        }
        addresses.push(n_addresses);
    }
    for i in 0..n {
        endpoints.push(Endpoint {
            addresses: addresses[i].clone(),
            ..Default::default()
        })
    }
    endpoints
}

fn create_n_endpoints_with_k_overlapping_addresses(n: usize, k: usize) -> Vec<Endpoint> {
    let mut addresses = Vec::new();
    let mut endpoints = Vec::new();
    for i in 0..n {
        let mut n_addresses = Vec::new();
        for j in 0..k {
            n_addresses.push(Address {
                address: format!("{}.{}.{}.{}:{}", j + i, j + i, j + i, j + i, j + i),
                // address: format!("{}.{}.{}.{}:{}", j, j, j, j, j),
                ..Default::default()
            });
        }
        addresses.push(n_addresses);
    }
    for i in 0..n {
        endpoints.push(Endpoint {
            addresses: addresses[i].clone(),
            ..Default::default()
        })
    }
    endpoints
}

fn create_n_endpoint_with_one_address(n: usize) -> Vec<Endpoint> {
    let mut endpoints = Vec::new();
    for i in 0..n {
        let address = Address {
            address: format!("{}.{}.{}.{}:{}", i, i, i, i, i),
            ..Default::default()
        };
        endpoints.push(Endpoint {
            addresses: vec![address.clone()],
            ..Default::default()
        });
    }
    endpoints
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
    println!("verifying subchannel creation");
    let mut subchannels = Vec::new();
    // let mut seen: HashSet<Address> = HashSet::new();
    for address in addresses {
        match rx_events.recv().await.unwrap() {
            TestEvent::NewSubchannel(addr, sc) => {
                // assert!(addr == address.clone());
                subchannels.push(sc);
            }
            other => panic!("unexpected event {}", other),
        };
    }
    subchannels
}

// Verifies that the subchannels are created for the given addresses in the
// given order. Returns the subchannels created.
async fn verify_multi_endpoint_subchannel_creation_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    mut addresses: Vec<Address>,
) -> Vec<Arc<dyn Subchannel>> {
    println!("verifying subchannel creation");
    let mut subchannels = Vec::new();
    for _ in 0..addresses.len() {
        match rx_events.recv().await.unwrap() {
            TestEvent::NewSubchannel(addr, sc) => {
                // Find and remove the address from the expected list
                if let Some(pos) = addresses.iter().position(|a| *a == addr) {
                    addresses.remove(pos);
                    subchannels.push(sc);
                } else {
                    panic!("unexpected subchannel address: {:?}", addr);
                }
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

// Sends initial subchannel updates to the LB policy for the given
// subchannels, with their state set to ready.
fn send_ready_subchannel_updates_to_policy(
    lb_policy: &mut dyn LbPolicy,
    subchannels: &[Arc<dyn Subchannel>],
    tcc: &mut dyn ChannelController,
) {
    for sc in subchannels {
        lb_policy.subchannel_update(sc.clone(), &SubchannelState::ready(), tcc);
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
    println!("Verify connection attempt");

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
    println!("verify schedule work from policy");

    match rx_events.recv().await.unwrap() {
        TestEvent::ScheduleWork => {}
        other => panic!("unexpected event {}", other),
    };
}

// Verifies that the channel moves to CONNECTING state with a queuing picker.
//
// Returns the picker for tests to make more picks, if required.
async fn verify_connecting_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
) -> Arc<dyn Picker> {
    println!("verify connecting picker");
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            println!("connectivity state is {}", update.connectivity_state);
            assert!(update.connectivity_state == ConnectivityState::Connecting);
            let req = test_utils::new_request();
            assert!(update.picker.pick(&req) == PickResult::Queue);
            return update.picker.clone();
        }
        other => panic!("unexpected event {}", other),
    }
}

// Verifies that the channel moves to READY state with a picker that returns the
// given subchannel.
//
// Returns the picker for tests to make more picks, if required.

//need to update this? only takes care of one subchannel
async fn verify_ready_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    subchannel: Arc<dyn Subchannel>,
) -> Arc<dyn Picker> {
    println!("verify ready picker");
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            println!(
                "connectivity state for ready picker is {}",
                update.connectivity_state
            );
            assert!(update.connectivity_state == ConnectivityState::Ready);
            let req = test_utils::new_request();
            match update.picker.pick(&req) {
                PickResult::Pick(pick) => {
                    println!("selected subchannel is {}", pick.subchannel);
                    println!("should've been selected subchannel is {}", subchannel);
                    assert!(pick.subchannel == subchannel.clone());
                    update.picker.clone()
                }
                other => panic!("unexpected pick result {}", other),
            }
        }
        other => panic!("unexpected event {}", other),
    }
}

async fn verify_roundrobin_ready_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    subchannel: Arc<dyn Subchannel>,
) -> Arc<dyn Picker> {
    println!("verify ready picker");
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            println!(
                "connectivity state for ready picker is {}",
                update.connectivity_state
            );
            assert!(update.connectivity_state == ConnectivityState::Ready);
            let req = test_utils::new_request();
            match update.picker.pick(&req) {
                PickResult::Pick(pick) => update.picker.clone(),
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
                    // assert!(status.message().contains(&want_error));
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
    println!("verifying resolution request");
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
async fn roundrobin_resolver_error_before_a_valid_update() {
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
async fn roundrobin_resolver_error_after_a_valid_update_in_ready() {
    //TODO: think about whether to use a bool for the general thing or not
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let failed = true;
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);

    println!("verrifying ready picker");
    let picker = verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

    println!("resolver error");
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

//use debug macro

#[tokio::test]
async fn roundrobin_resolver_error_after_a_valid_update_in_connecting() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    println!("created endpoint");
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    println!("send_resolver_update_to_policy");
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    println!("printing subchannels created in the test");
    for sc in &subchannels {
        println!("  Subchannel: {}", sc);
    }
    println!("verifying subchannel creation");
    println!("calling send_initial_subchannel_updates_to_policy");

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    // verify_connecting_picker_from_policy(&mut rx_events).await;
    println!("send_initial_subchannel_updates_to_policy");
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    let picker = verify_connecting_picker_from_policy(&mut rx_events).await;

    println!("move_subchannel_to_connecting");
    //this is not happening... need to fix this so either child manager or child manager
    println!("verify_connecting_picker_from_policy");
    //no capture with cargo test
    let resolver_error = String::from("resolver error");

    println!("send resolver error");
    send_resolver_error_to_policy(lb_policy, resolver_error.clone(), tcc);

    println!("verify no activity from policy");
    verify_no_activity_from_policy(&mut rx_events).await;

    println!("check pick result");
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
async fn roundrobin_resolver_error_after_a_valid_update_in_tf() {
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
    verify_resolution_request(&mut rx_events).await;
    verify_transient_failure_picker_from_policy(&mut rx_events, connection_error).await;

    let resolver_error = String::from("resolver error");
    send_resolver_error_to_policy(lb_policy, resolver_error.clone(), tcc);
    verify_transient_failure_picker_from_policy(&mut rx_events, resolver_error).await;
}

#[tokio::test]
async fn roundrobin_simple_test() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);
    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, subchannels[1].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
}

// Round Robin should move into Ready as soon as one of its children
// becomes ready.
#[tokio::test]
async fn roundrobin_aggregation_ready() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_overlapping_addresses(2, 3);
    send_resolver_update_to_policy(lb_policy, endpoints.clone(), tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;

    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());
    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    verify_connection_attempt_from_policy(&mut rx_events, second_subchannels[0].clone()).await;
    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_connecting(lb_policy, second_subchannels[0].clone(), tcc);

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
}

// Round Robin should round robin across endpoints.
#[tokio::test]
async fn roundrobin_picks_are_round_robin() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_addresses(2, 1);
    send_resolver_update_to_policy(lb_policy, endpoints.clone(), tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;

    // Flatten all subchannels into one list
    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());
    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);
    for sc in &all_subchannels {
        verify_connection_attempt_from_policy(&mut rx_events, sc.clone()).await;
    }

    move_subchannel_to_connecting(lb_policy, all_subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, all_subchannels[0].clone(), tcc);
    verify_roundrobin_ready_picker_from_policy(&mut rx_events, all_subchannels[0].clone()).await;
    move_subchannel_to_ready(lb_policy, all_subchannels[1].clone(), tcc);
    // verify_roundrobin_ready_picker_from_policy(&mut rx_events, all_subchannels[1].clone()).await;

    // Get the picker and make several picks
    let picker = {
        let req = test_utils::new_request();
        match rx_events.recv().await.unwrap() {
            TestEvent::UpdatePicker(update) => update.picker.clone(),
            other => panic!("unexpected event {}", other),
        }
    };

    // Make 4 picks and collect the picked subchannels
    let req = test_utils::new_request();
    let mut picked = Vec::new();
    for _ in 0..4 {
        match picker.pick(&req) {
            PickResult::Pick(pick) => {
                println!("picked subchannel is {}", pick.subchannel);
                picked.push(pick.subchannel.clone())
            }
            other => panic!("unexpected pick result {}", other),
        }
    }

    // Both subchannels should appear at least once in the first two picks
    assert!(
        picked[0] != picked[1].clone(),
        "Should alternate between subchannels"
    );
    // The sequence should repeat every two picks
    assert_eq!(&picked[0], &picked[2]);
    assert_eq!(&picked[1], &picked[3]);
    // Both subchannels should be present
    assert!(picked.contains(&subchannels[0]));
    assert!(picked.contains(&second_subchannels[0]));
}

// If round robin receives no addresses in a resolver update,
// it should go into transient failure.
#[tokio::test]
async fn roundrobin_addresses_removed() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_overlapping_addresses(2, 3);
    send_resolver_update_to_policy(lb_policy, endpoints.clone(), tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;

    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());
    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    verify_connection_attempt_from_policy(&mut rx_events, second_subchannels[0].clone()).await;
    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_connecting(lb_policy, second_subchannels[0].clone(), tcc);
    let update = ResolverUpdate {
        endpoints: Ok(vec![]),
        ..Default::default()
    };
    assert!(lb_policy.resolver_update(update, None, tcc).is_err());

    let want_error = "no addresses are given";
    verify_transient_failure_picker_from_policy(&mut rx_events, want_error.to_string()).await;
}

// Round robin should only round robin across children that are ready.
// If a child leaves the ready state, Round Robin should only
// pick from the children that are still Ready.
#[tokio::test]
async fn roundrobin_one_endpoint_down() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_addresses(2, 1);
    send_resolver_update_to_policy(lb_policy, endpoints.clone(), tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;

    // Flatten all subchannels into one list
    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());
    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);
    for sc in &all_subchannels {
        verify_connection_attempt_from_policy(&mut rx_events, sc.clone()).await;
    }

    move_subchannel_to_connecting(lb_policy, all_subchannels[0].clone(), tcc);
    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_ready(lb_policy, all_subchannels[0].clone(), tcc);
    verify_roundrobin_ready_picker_from_policy(&mut rx_events, all_subchannels[0].clone()).await;
    move_subchannel_to_ready(lb_policy, all_subchannels[1].clone(), tcc);
    // Get the picker and make several picks
    let picker = {
        let req = test_utils::new_request();
        match rx_events.recv().await.unwrap() {
            TestEvent::UpdatePicker(update) => update.picker.clone(),
            other => panic!("unexpected event {}", other),
        }
    };

    // Make 4 picks and collect the picked subchannels
    let req = test_utils::new_request();
    let mut picked = Vec::new();
    for _ in 0..4 {
        match picker.pick(&req) {
            PickResult::Pick(pick) => {
                println!("picked subchannel is {}", pick.subchannel);
                picked.push(pick.subchannel.clone())
            }
            other => panic!("unexpected pick result {}", other),
        }
    }

    // Both subchannels should appear at least once in the first two picks
    assert!(
        picked[0] != picked[1].clone(),
        "Should alternate between subchannels"
    );
    // The sequence should repeat every two picks
    assert_eq!(&picked[0], &picked[2]);
    assert_eq!(&picked[1], &picked[3]);
    // Both subchannels should be present
    assert!(picked.contains(&subchannels[0]));
    assert!(picked.contains(&second_subchannels[0]));
    let subchannel_being_removed = all_subchannels[1].clone();
    move_subchannel_to_idle(lb_policy, all_subchannels[1].clone(), tcc);
    verify_resolution_request(&mut rx_events).await;
    let subchannel =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;
    verify_connection_attempt_from_policy(&mut rx_events, subchannel[0].clone()).await;

    let new_picker =
        verify_roundrobin_ready_picker_from_policy(&mut rx_events, all_subchannels[0].clone())
            .await;
    let req = test_utils::new_request();
    let mut picked = Vec::new();
    for _ in 0..4 {
        match new_picker.pick(&req) {
            PickResult::Pick(pick) => {
                println!("picked subchannel is {}", pick.subchannel);
                picked.push(pick.subchannel.clone())
            }
            other => panic!("unexpected pick result {}", other),
        }
    }

    assert_eq!(&picked[0], &picked[2]);
    assert_eq!(&picked[1], &picked[3]);
    assert!(picked.contains(&subchannels[0]));
    assert!(!picked.contains(&subchannel_being_removed));
}

// If Round Robin receives a resolver update that removes an endpoint and adds a new endpoint
// from a previous update, that endpoint's subchannels should not be apart of its
// picks anymore and should be removed. It should then roundrobin across the endpoints it still has
// and the new one.
#[tokio::test]
async fn roundrobin_pick_after_resolved_updated_hosts() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    // Step 1: Initial endpoints: removed, old
    let removed_addr = Address {
        address: "removed".to_string(),
        ..Default::default()
    };
    let old_addr = Address {
        address: "old".to_string(),
        ..Default::default()
    };
    let removed_endpoint = Endpoint {
        addresses: vec![removed_addr.clone()],
        ..Default::default()
    };
    let old_endpoint = Endpoint {
        addresses: vec![old_addr.clone()],
        ..Default::default()
    };

    // Initial update: [removed, old]
    send_resolver_update_to_policy(
        lb_policy,
        vec![removed_endpoint.clone(), old_endpoint.clone()],
        tcc,
    );

    // Create subchannels for both
    let mut all_addresses = removed_endpoint.addresses.clone();
    all_addresses.extend(old_endpoint.addresses.clone());
    let all_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, all_addresses).await;
    let removed_sc = all_subchannels
        .iter()
        .find(|sc| sc.address().address == "removed")
        .unwrap()
        .clone();
    let old_sc = all_subchannels
        .iter()
        .find(|sc| sc.address().address == "old")
        .unwrap()
        .clone();
    println!("removed_subchannels[0] address: {}", removed_sc.address());
    println!("old_subchannels[0] address: {}", old_sc.address());

    // Set both to IDLE, then CONNECTING, then READY
    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);
    move_subchannel_to_connecting(lb_policy, removed_sc.clone(), tcc);
    verify_connection_attempt_from_policy(&mut rx_events, all_subchannels[0].clone()).await;
    verify_connection_attempt_from_policy(&mut rx_events, all_subchannels[1].clone()).await;

    verify_connecting_picker_from_policy(&mut rx_events).await;
    move_subchannel_to_connecting(lb_policy, old_sc.clone(), tcc);
    move_subchannel_to_ready(lb_policy, removed_sc.clone(), tcc);
    verify_roundrobin_ready_picker_from_policy(&mut rx_events, removed_sc.clone()).await;
    move_subchannel_to_ready(lb_policy, old_sc.clone(), tcc);

    // Picker should contain both subchannels
    let picker =
        verify_roundrobin_ready_picker_from_policy(&mut rx_events, removed_sc.clone()).await;
    let req = test_utils::new_request();
    let mut picked = Vec::new();
    for _ in 0..4 {
        match picker.pick(&req) {
            PickResult::Pick(pick) => {
                println!("picker subchannel is {}", pick.subchannel.clone());
                picked.push(pick.subchannel.clone())
            }
            other => panic!("unexpected pick result {}", other),
        }
    }
    assert!(picked.contains(&removed_sc));
    assert!(picked.contains(&old_sc));

    // Step 2: Resolver update: [old (with new attr), new]

    let new_addr = Address {
        address: "new".to_string(),
        ..Default::default()
    };
    let new_endpoint = Endpoint {
        addresses: vec![new_addr.clone()],
        ..Default::default()
    };
    let mut all_new_addresses = old_endpoint.addresses.clone();
    all_new_addresses.extend(new_endpoint.addresses.clone());

    send_resolver_update_to_policy(
        lb_policy,
        vec![old_endpoint.clone(), new_endpoint.clone()],
        tcc,
    );
    let new_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, all_new_addresses.clone()).await;
    let old_sc = new_subchannels
        .iter()
        .find(|sc| sc.address().address == "old")
        .unwrap()
        .clone();
    let new_sc = new_subchannels
        .iter()
        .find(|sc| sc.address().address == "new")
        .unwrap()
        .clone();

    lb_policy.subchannel_update(
        old_sc.clone(),
        &SubchannelState {
            connectivity_state: ConnectivityState::Ready,
            ..Default::default()
        },
        tcc,
    );
    let picker = verify_ready_picker_from_policy(&mut rx_events, old_sc.clone()).await;
    println!("new subchannels is {}", new_subchannels[0]);

    send_initial_subchannel_updates_to_policy(lb_policy, &vec![new_sc.clone()], tcc);

    move_subchannel_to_connecting(lb_policy, new_sc.clone(), tcc);
    verify_connection_attempt_from_policy(&mut rx_events, new_sc.clone()).await;
    move_subchannel_to_ready(lb_policy, new_sc.clone(), tcc);

    // Picker should now contain only old and new (removed should be gone)

    let new_picker =
        verify_roundrobin_ready_picker_from_policy(&mut rx_events, new_sc.clone()).await;
    let req = test_utils::new_request();
    let mut picked = Vec::new();
    for _ in 0..4 {
        match new_picker.pick(&req) {
            PickResult::Pick(pick) => {
                println!("pick is {}", pick.subchannel);
                picked.push(pick.subchannel.clone())
            }
            other => panic!("unexpected pick result {}", other),
        }
    }
    assert!(picked.contains(&old_sc));
    assert!(picked.contains(&new_sc));
    assert!(!picked.contains(&removed_sc));
}

// Round robin should be in Ready if at least one child is ready.
// However, if then all children become connecting, Round Robin
// should move to connecting.
#[tokio::test]
async fn roundrobin_aggregationing_ready_to_connecting() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_addresses(2, 1);

    send_resolver_update_to_policy(lb_policy, endpoints.clone(), tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;

    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());
    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    verify_connection_attempt_from_policy(&mut rx_events, second_subchannels[0].clone()).await;
    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_connecting(lb_policy, second_subchannels[0].clone(), tcc);

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_ready(lb_policy, second_subchannels[0].clone(), tcc);
    let picker =
        verify_roundrobin_ready_picker_from_policy(&mut rx_events, second_subchannels[0].clone())
            .await;
    let req = test_utils::new_request();
    let pick1 = match picker.pick(&req) {
        PickResult::Pick(p) => p.subchannel.clone(),
        other => panic!("unexpected pick result {}", other),
    };
    let pick2 = match picker.pick(&req) {
        PickResult::Pick(p) => p.subchannel.clone(),
        other => panic!("unexpected pick result {}", other),
    };
    let picked = vec![pick1, pick2];
    assert!(picked.contains(&subchannels[0]));
    assert!(picked.contains(&second_subchannels[0]));

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_resolution_request(&mut rx_events).await;
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    verify_ready_picker_from_policy(&mut rx_events, second_subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, second_subchannels[0].clone(), tcc);
    verify_resolution_request(&mut rx_events).await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;
    verify_connection_attempt_from_policy(&mut rx_events, second_subchannels[0].clone()).await;

    verify_connecting_picker_from_policy(&mut rx_events).await;
}

// When all children are in transient failure, round robin should move to transient failure.
#[tokio::test]
async fn roundrobin_aggregation_transient_failure() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_addresses(2, 1);

    send_resolver_update_to_policy(lb_policy, endpoints.clone(), tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;

    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());

    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    verify_connection_attempt_from_policy(&mut rx_events, second_subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_connecting(lb_policy, second_subchannels[0].clone(), tcc);

    move_subchannel_to_transient_failure(lb_policy, second_subchannels[0].clone(), "oops", tcc);

    move_subchannel_to_transient_failure(lb_policy, subchannels[0].clone(), "oops", tcc);

    verify_resolution_request(&mut rx_events).await;
    verify_resolution_request(&mut rx_events).await;
    verify_transient_failure_picker_from_policy(&mut rx_events, "oops".to_string()).await;
}

// Testing aggregation logic of moving from transient failure to ready.
// When all children are in transient failure, round robin is in transient failure.
// As soon as a child goes to ready, round robin should also move to ready.
#[tokio::test]
async fn roundrobin_aggregation_transient_failure_to_ready() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_addresses(2, 1);

    send_resolver_update_to_policy(lb_policy, endpoints.clone(), tcc);

    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;

    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());

    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    verify_connection_attempt_from_policy(&mut rx_events, second_subchannels[0].clone()).await;

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_connecting(lb_policy, second_subchannels[0].clone(), tcc);

    move_subchannel_to_transient_failure(lb_policy, second_subchannels[0].clone(), "oops", tcc);

    move_subchannel_to_transient_failure(lb_policy, subchannels[0].clone(), "oops", tcc);

    verify_resolution_request(&mut rx_events).await;
    verify_resolution_request(&mut rx_events).await;
    verify_transient_failure_picker_from_policy(&mut rx_events, "oops".to_string()).await;
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_ready(lb_policy, second_subchannels[0].clone(), tcc);
    verify_roundrobin_ready_picker_from_policy(&mut rx_events, second_subchannels[0].clone()).await;
}

#[tokio::test]
async fn roundrobin_zero_addresses_from_resolver_before_valid_update() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_addresses(4, 0);
    let update = ResolverUpdate {
        endpoints: Ok(endpoints.clone()),
        ..Default::default()
    };
    assert!(lb_policy.resolver_update(update, None, tcc).is_err());
    verify_transient_failure_picker_from_policy(
        &mut rx_events,
        "received empty address list from the name resolver".to_string(),
    )
    .await;
}

//Round robin should stay in transient failure until a child reports ready
#[tokio::test]
async fn roundrobin_stay_transient_failure_until_ready() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);
    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_connecting(lb_policy, subchannels[1].clone(), tcc);
    let first_error = String::from("test connection error 1");
    for sc in &subchannels {
        move_subchannel_to_transient_failure(lb_policy, sc.clone(), &first_error, tcc);
    }

    println!("verifying transient failure picker");
    verify_resolution_request(&mut rx_events).await;
    verify_transient_failure_picker_from_policy(&mut rx_events, first_error.clone()).await;

    println!("verifying resolution request");

    println!("moving subchannel to ready");
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);

    println!("verifying subchannel to ready");
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
}

// Tests the scenario where the resolver returns an update with no endpoints
// (before sending any valid update). The LB policy should move to
// TRANSIENT_FAILURE state with a failing picker.
#[tokio::test]
async fn roundrobin_zero_endpoints_from_resolver_before_valid_update() {
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
async fn roundrobin_zero_endpoints_from_resolver_after_valid_update() {
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
async fn roundrobin_with_one_backend() {
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
async fn roundrobin_with_multiple_backends_first_backend_is_ready() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoint = create_endpoint_with_n_addresses(2);
    send_resolver_update_to_policy(lb_policy, vec![endpoint.clone()], tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    send_initial_subchannel_updates_to_policy(lb_policy, &subchannels, tcc);

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);

    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
}

// Tests the scenario where the resolver returns an update with multiple
// address, some of which are duplicates. The LB policy should dedup the
// addresses and create subchannels for them, and attempt to connect to them in
// order, until a connection succeeds, at which point it should move to READY
// state with a picker that returns that subchannel.
#[tokio::test]
async fn roundrobin_with_multiple_backends_duplicate_addresses() {
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
// addresses and the LB policy successfully connects to first one and moves to
// READY. The resolver then returns an update with a new address list that
// contains the address of the currently connected subchannel. The LB policy
// should create subchannels for the new addresses, and then see that the
// currently connected subchannel is in the new address list. It should then
// send a new READY picker that returns the currently connected subchannel.
#[tokio::test]
async fn roundrobin_resolver_update_contains_currently_ready_subchannel() {
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
async fn roundrobin_resolver_update_contains_identical_address_list() {
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

//Roundrobin should round robin across three endpoints
#[tokio::test]
async fn roundrobin_logic_three_endpoints() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_addresses(3, 1);
    send_resolver_update_to_policy(lb_policy, endpoints.clone(), tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;
    let third_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[2].addresses.clone())
            .await;

    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());
    all_subchannels.extend(third_subchannels.clone());
    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);

    move_subchannel_to_connecting(lb_policy, subchannels[0].clone(), tcc);

    verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    verify_connection_attempt_from_policy(&mut rx_events, second_subchannels[0].clone()).await;
    verify_connection_attempt_from_policy(&mut rx_events, third_subchannels[0].clone()).await;
    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_connecting(lb_policy, second_subchannels[0].clone(), tcc);
    move_subchannel_to_connecting(lb_policy, third_subchannels[0].clone(), tcc);
    move_subchannel_to_ready(lb_policy, subchannels[0].clone(), tcc);
    verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
    move_subchannel_to_ready(lb_policy, second_subchannels[0].clone(), tcc);
    let picker =
        verify_roundrobin_ready_picker_from_policy(&mut rx_events, second_subchannels[0].clone())
            .await;
    move_subchannel_to_ready(lb_policy, third_subchannels[0].clone(), tcc);
    let picker =
        verify_roundrobin_ready_picker_from_policy(&mut rx_events, third_subchannels[0].clone())
            .await;
    let req = test_utils::new_request();
    let pick1 = match picker.pick(&req) {
        PickResult::Pick(p) => {
            println!("p.subchannel for pick1 is {}", p.subchannel);
            p.subchannel.clone()
        }
        other => panic!("unexpected pick result {}", other),
    };
    let pick2 = match picker.pick(&req) {
        PickResult::Pick(p) => {
            println!("p.subchannel for pick2 is {}", p.subchannel);
            p.subchannel.clone()
        }
        other => panic!("unexpected pick result {}", other),
    };
    let pick3 = match picker.pick(&req) {
        PickResult::Pick(p) => {
            println!("p.subchannel for pick3 is {}", p.subchannel);
            p.subchannel.clone()
        }
        other => panic!("unexpected pick result {}", other),
    };
    let pick4 = match picker.pick(&req) {
        PickResult::Pick(p) => {
            println!("p.subchannel for pick4 is {}", p.subchannel);
            p.subchannel.clone()
        }
        other => panic!("unexpected pick result {}", other),
    };
    let picked = vec![pick1, pick2, pick3];
    let looped_pick = vec![pick4];
    assert!(picked.contains(&subchannels[0]));
    assert!(picked.contains(&second_subchannels[0]));
    assert!(picked.contains(&third_subchannels[0]));
    assert!(looped_pick.contains(&subchannels[0]));
}

//Roundrobin should move from ready to idle to transient failure and then to ready
#[tokio::test]
async fn roundrobin_ready_to_idle_to_transient_failure_to_ready() {
    let (mut rx_events, mut lb_policy, mut tcc) = setup();
    let lb_policy = lb_policy.as_mut();
    let tcc = tcc.as_mut();

    let endpoints = create_n_endpoints_with_k_addresses(2, 1);
    send_resolver_update_to_policy(lb_policy, endpoints.clone(), tcc);
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;
    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());
    send_initial_subchannel_updates_to_policy(lb_policy, &all_subchannels, tcc);

    move_subchannel_to_connecting(lb_policy, all_subchannels[0].clone(), tcc);

    verify_connection_attempt_from_policy(&mut rx_events, all_subchannels[0].clone()).await;
    verify_connection_attempt_from_policy(&mut rx_events, all_subchannels[1].clone()).await;
    verify_connecting_picker_from_policy(&mut rx_events).await;

    move_subchannel_to_ready(lb_policy, all_subchannels[0].clone(), tcc);

    verify_ready_picker_from_policy(&mut rx_events, all_subchannels[0].clone()).await;
    move_subchannel_to_connecting(lb_policy, all_subchannels[1].clone(), tcc);
    move_subchannel_to_ready(lb_policy, all_subchannels[1].clone(), tcc);

    let picker =
        verify_roundrobin_ready_picker_from_policy(&mut rx_events, second_subchannels[0].clone())
            .await;
    let req = test_utils::new_request();
    let pick1 = match picker.pick(&req) {
        PickResult::Pick(p) => p.subchannel.clone(),
        other => panic!("unexpected pick result {}", other),
    };
    let pick2 = match picker.pick(&req) {
        PickResult::Pick(p) => p.subchannel.clone(),
        other => panic!("unexpected pick result {}", other),
    };
    let picked = vec![pick1, pick2];
    assert!(picked.contains(&subchannels[0]));
    assert!(picked.contains(&second_subchannels[0]));

    let first_error = String::from("test connection error 1");
    move_subchannel_to_transient_failure(lb_policy, all_subchannels[0].clone(), &first_error, tcc);
    verify_resolution_request(&mut rx_events).await;
    let subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[0].addresses.clone())
            .await;
    verify_connection_attempt_from_policy(&mut rx_events, all_subchannels[0].clone()).await;
    verify_ready_picker_from_policy(&mut rx_events, all_subchannels[1].clone()).await;
    println!("verifying transient failure picker");
    move_subchannel_to_transient_failure(
        lb_policy,
        second_subchannels[0].clone(),
        &first_error,
        tcc,
    );
    verify_resolution_request(&mut rx_events).await;
    let second_subchannels =
        verify_subchannel_creation_from_policy(&mut rx_events, endpoints[1].addresses.clone())
            .await;
    verify_connection_attempt_from_policy(&mut rx_events, second_subchannels[0].clone()).await;
    verify_connecting_picker_from_policy(&mut rx_events).await;
    let mut all_subchannels = subchannels.clone();
    all_subchannels.extend(second_subchannels.clone());

    move_subchannel_to_ready(lb_policy, all_subchannels[0].clone(), tcc);

    println!("verifying subchannel to ready");
    let picker =
        verify_roundrobin_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
    let req = test_utils::new_request();
    let pick1 = match picker.pick(&req) {
        PickResult::Pick(p) => p.subchannel.clone(),
        other => panic!("unexpected pick result {}", other),
    };
    let pick2 = match picker.pick(&req) {
        PickResult::Pick(p) => p.subchannel.clone(),
        other => panic!("unexpected pick result {}", other),
    };
    let picked = vec![pick1, pick2];
    assert!(picked.contains(&subchannels[0]));
    assert!(!picked.contains(&second_subchannels[0]));
    move_subchannel_to_ready(lb_policy, second_subchannels[0].clone(), tcc);

    let picker =
        verify_roundrobin_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
    let req = test_utils::new_request();
    let pick1 = match picker.pick(&req) {
        PickResult::Pick(p) => p.subchannel.clone(),
        other => panic!("unexpected pick result {}", other),
    };
    let pick2 = match picker.pick(&req) {
        PickResult::Pick(p) => p.subchannel.clone(),
        other => panic!("unexpected pick result {}", other),
    };
    let picked = vec![pick1, pick2];
    assert!(picked.contains(&subchannels[0]));
    assert!(picked.contains(&second_subchannels[0]));
}
