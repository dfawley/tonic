use std::{panic, sync::Arc};

use tokio::sync::mpsc;

use crate::client::{load_balancing::
    {graceful_switch::{self, GracefulSwitchPolicy}, pick_first, round_robin::{self, RoundRobinPicker,}, test_utils::{self, FakeChannel, MockBalancerOne, MockBalancerTwo, MockPickerOne, MockPickerTwo, TestEvent, TestWorkScheduler}, ChannelController, LbPolicy, LbPolicyBuilder, LbPolicyOptions, ParsedJsonLbConfig, PickResult, Picker, Subchannel, SubchannelState, GLOBAL_LB_REGISTRY}, 
    name_resolution::{Address, Endpoint, ResolverUpdate}, service_config::ServiceConfig, ConnectivityState};


impl GracefulSwitchPolicy {
    /// Returns the name of the current policy, if any.
    pub fn current_policy_name(&self) -> Option<String> {
        self.current_policy_builder
            .lock()
            .unwrap()
            .as_ref()
            .map(|b| b.name().to_string())
    }

    /// Returns the name of the pending policy, if any.
    pub fn pending_policy_name(&self) -> Option<String> {
        self.pending_policy_builder
            .lock()
            .unwrap()
            .as_ref()
            .map(|b| b.name().to_string())
    }
}

fn setup() -> (
    mpsc::UnboundedReceiver<TestEvent>,
    Box< GracefulSwitchPolicy>,
    // Box<dyn LbPolicy>,
    // Box<dyn LbPolicy>,
    Box<dyn ChannelController>,
) {
    pick_first::reg();

    round_robin::reg();

    test_utils::reg();
    
    let (tx_events, rx_events) = mpsc::unbounded_channel::<TestEvent>();
    let work_scheduler = Arc::new(TestWorkScheduler {
        tx_events: tx_events.clone(),
    });
    // let tcc = Box::new(FakeChannel::new(tx_events.clone()));
    let tcc = Box::new(FakeChannel {
        tx_events: tx_events.clone(),
    });
    // let builder: Arc<dyn LbPolicyBuilder> = GLOBAL_LB_REGISTRY.get_policy("round_robin").unwrap();
    // let lb_policy = builder.build(LbPolicyOptions { work_scheduler: work_scheduler.clone() });
    // let second_builder: Arc<dyn LbPolicyBuilder> = GLOBAL_LB_REGISTRY.get_policy("pick_first").unwrap();
    // let second_lb_policy = builder.build(LbPolicyOptions { work_scheduler: work_scheduler.clone() });
    let graceful_switch = GracefulSwitchPolicy::new(work_scheduler.clone());
    (rx_events, Box::new(graceful_switch), tcc)
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

async fn verify_roundrobin_ready_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    subchannel: Arc<dyn Subchannel>,
) -> Arc<dyn Picker> {
    println!("verify ready picker");
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            println!("connectivity state for ready picker is {}", update.connectivity_state);
            assert!(update.connectivity_state == ConnectivityState::Ready);
            // assert!(update.picker == ReadyPickers);
            let req = test_utils::new_request();
            match update.picker.pick(&req) {
                PickResult::Pick(pick) => {
                    // println!("selected subchannel is {}", pick.subchannel);
                    // println!("should've been selected subchannel is {}", subchannel);
                    assert!(pick.subchannel == subchannel.clone());
                    update.picker.clone()
                }
                other => panic!("unexpected pick result {}", other),
            }
        }
        other => panic!("unexpected event {}", other),
    }
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

// Verifies that the channel moves to READY state with a picker that returns the
// given subchannel.
//
// Returns the picker for tests to make more picks, if required.

//need to update this? only takes care of one subchannel
async fn verify_mock_policy_one_ready_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    subchannel: Arc<dyn Subchannel>,
) -> Arc<dyn Picker> {
    println!("verify ready picker");
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            assert!(update.connectivity_state == ConnectivityState::Ready);
            let req = test_utils::new_request();
            
            match update.picker.pick(&req) {
                PickResult::Pick(pick) => {
                    assert!(pick.subchannel.address().address == "one");
                    update.picker.clone()
                }
                other => panic!("unexpected pick result {}", other),
            }
        }
        other => panic!("unexpected event {}", other),
    }
}

// Verifies that the channel moves to READY state with a picker that returns the
// given subchannel.
//
// Returns the picker for tests to make more picks, if required.

//need to update this? only takes care of one subchannel
async fn verify_mock_policy_two_ready_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
    subchannel: Arc<dyn Subchannel>,
) -> Arc<dyn Picker> {
    match rx_events.recv().await.unwrap() {
        TestEvent::UpdatePicker(update) => {
            assert!(update.connectivity_state == ConnectivityState::Ready);
            let req = test_utils::new_request();
            
            match update.picker.pick(&req) {
                PickResult::Pick(pick) => {
                    assert!(pick.subchannel.address().address == "two");
                    update.picker.clone()
                }
                other => panic!("unexpected pick result {}", other),
            }
        }
        other => panic!("unexpected event {}", other),
    }
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

// Verifies that the channel moves to CONNECTING state with a queuing picker.
//
// Returns the picker for tests to make more picks, if required.
async fn verify_mock_connecting_picker_from_policy(
    rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
) -> Arc<dyn Picker> {
    println!("verify connecting picker");
    match rx_events.recv().await.unwrap() {
 
        TestEvent::UpdatePicker(update) => {
            println!("connectivity state is {}", update.connectivity_state);
            assert!(update.connectivity_state == ConnectivityState::Connecting);
            let req = test_utils::new_request();
            return update.picker.clone();
        }
        other => panic!("unexpected event {}", other),
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

#[tokio::test]
async fn gracefulswitch_successful_first_update() {
    let (mut rx_events, mut graceful_switch, mut tcc) = setup();
    let service_config = serde_json::json!({
        "children_policies": [
            { "round_robin": serde_json::json!({}) },
            { "pick_first": serde_json::json!({ "shuffleAddressList": false }) }
        ]
    });
    

    let parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config)).unwrap().unwrap();

    let endpoint = create_endpoint_with_one_address("127.0.0.1:1234".to_string());
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint.clone()]),
        ..Default::default()
    };
    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );
    let roundrobin_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    assert_eq!(roundrobin_subchannels.len(), 1);
    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should still be round_robin after second update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should still be None after second update"
    );
}



//Testing that a pending policy is created
#[tokio::test]
async fn gracefulswitch_switching_to_resolver_update() {
    let (mut rx_events, mut graceful_switch, mut tcc) = setup();
    let service_config = serde_json::json!({
        "children_policies": [
            { "round_robin": serde_json::json!({}) },
            { "pick_first": serde_json::json!({ "shuffleAddressList": false }) }
        ]
    });
    // let service_config = ServiceConfig::from_json(&serde_json::to_value(&service_config).unwrap()).unwrap();

    let parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config)).unwrap().unwrap();

    let endpoint = create_endpoint_with_one_address("127.0.0.1:1234".to_string());
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint.clone()]),
        ..Default::default()
    };
    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );

    let roundrobin_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(&mut *graceful_switch, &roundrobin_subchannels, tcc.as_mut());
    move_subchannel_to_ready(&mut *graceful_switch, roundrobin_subchannels[0].clone(), tcc.as_mut());
    let new_service_config = serde_json::json!({
        "children_policies": [
            { "pick_first": serde_json::json!({ "shuffleAddressList": false }) },
        ]
    });

    let new_parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(new_service_config)).unwrap().unwrap();
    graceful_switch
        .resolver_update(update.clone(), Some(&new_parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        Some("pick_first"),
        "Pending policy should be None after first update"
    );
}

//Testing that a pending policy is created
#[tokio::test]
async fn gracefulswitch_mock_switching_to_resolver_update() {
    let (mut rx_events, mut graceful_switch, mut tcc) = setup();

    // 1. Start with mock_policy_one as current
    let service_config = serde_json::json!({
        "children_policies": [
            { "mock_policy_one": serde_json::json!({}) }
        ]
    });
    let parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config)).unwrap().unwrap();

    let endpoint = create_endpoint_with_one_address("127.0.0.1:1234".to_string());
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint.clone()]),
        ..Default::default()
    };

    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("mock_policy_one"),
        "Current policy should be mock_policy_one after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );
    
    // Subchannel creation and ready
    let subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    verify_mock_connecting_picker_from_policy(&mut rx_events).await;
    // send_initial_subchannel_updates_to_policy(&mut *graceful_switch, &subchannels, tcc.as_mut());
    move_subchannel_to_ready(&mut *graceful_switch, subchannels[0].clone(), tcc.as_mut());

    // Assert picker is MockPickerOne by checking subchannel address
    let picker = verify_mock_policy_one_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
    let req = test_utils::new_request();
    match picker.pick(&req) {
        PickResult::Pick(pick) => {
            assert_eq!(pick.subchannel.address().address, "one", "Expected MockPickerOne to pick subchannel with address 'one'");
        }
        other => panic!("unexpected pick result"),
    }

    // 2. Switch to mock_policy_two as pending
    let new_service_config = serde_json::json!({
        "children_policies": [
            { "mock_policy_two": serde_json::json!({}) }
        ]
    });
    let new_parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(new_service_config)).unwrap().unwrap();
    graceful_switch
        .resolver_update(update.clone(), Some(&new_parsed_config), &mut *tcc)
        .unwrap();

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("mock_policy_one"),
        "Current policy should still be mock_policy_one before swap"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        Some("mock_policy_two"),
        "Pending policy should be mock_policy_two after switch"
    );

    // Simulate subchannel creation and ready for pending
    let subchannels_two = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    verify_mock_connecting_picker_from_policy(&mut rx_events).await;
    // send_initial_subchannel_updates_to_policy(&mut *graceful_switch, &subchannels_two, tcc.as_mut());
    move_subchannel_to_ready(&mut *graceful_switch, subchannels_two[0].clone(), tcc.as_mut());

    // Assert picker is MockPickerTwo by checking subchannel address
    let picker_two = verify_mock_policy_two_ready_picker_from_policy(&mut rx_events, subchannels_two[0].clone()).await;

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("mock_policy_two"),
        "Current policy should be mock_policy_two after swap"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after swap"
    );
}

#[tokio::test]
async fn gracefulswitch_two_balancers_same_type() {
    let (mut rx_events, mut graceful_switch, mut tcc) = setup();

    let service_config = serde_json::json!({
        "children_policies": [
            { "round_robin": serde_json::json!({}) }
        ]
    });
    let parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config)).unwrap().unwrap();

    let endpoint = create_endpoint_with_one_address("127.0.0.1:1234".to_string());
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint.clone()]),
        ..Default::default()
    };

    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );

    let roundrobin_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    move_subchannel_to_ready(&mut *graceful_switch, roundrobin_subchannels[0].clone(), tcc.as_mut());

    let picker = verify_roundrobin_ready_picker_from_policy(&mut rx_events, roundrobin_subchannels[0].clone()).await;

    let service_config2 = serde_json::json!({
        "children_policies": [
            { "round_robin": serde_json::json!({}) }
        ]
    });
    let parsed_config2 = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config2)).unwrap().unwrap();

    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config2), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );

}

#[tokio::test]
async fn gracefulswitch_current_not_ready_pending_update() {
    let (mut rx_events, mut graceful_switch, mut tcc) = setup();

    // 1. Initial switch to round_robin (acts as mockBalancerBuilder1)
    let service_config = serde_json::json!({
        "children_policies": [
            { "round_robin": serde_json::json!({}) }
        ]
    });
   
    let parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config)).unwrap().unwrap();

    let endpoint = create_endpoint_with_one_address("127.0.0.1:1234".to_string());
    let pickfirst_endpoint = create_endpoint_with_one_address("0.0.0.0.0".to_string());
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint.clone()]),
        ..Default::default()
    };

    // Switch to first round_robin (current)
    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );

    
    let current_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

    // 2. Switch to another round_robin (pending)
    let new_service_config = serde_json::json!({
        "children_policies": [
            { "pick_first": serde_json::json!({ "shuffleAddressList": false }) },
           
        ]
    });
    let pickfirst_update = ResolverUpdate {
        endpoints: Ok(vec![pickfirst_endpoint.clone()]),
        ..Default::default()
    };
    let new_parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(new_service_config)).unwrap().unwrap();
    graceful_switch
        .resolver_update(pickfirst_update.clone(), Some(&new_parsed_config), &mut *tcc)
        .unwrap();

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("pick_first"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        None,
        "Pending policy should be None after first update"
    );
    
 
}

#[tokio::test]
async fn gracefulswitch_subchannel_tracking() {
    let (mut rx_events, mut graceful_switch, mut tcc) = setup();

    // 1. Initial switch to round_robin (acts as mockBalancerBuilder1)
    let service_config = serde_json::json!({
        "children_policies": [
            { "round_robin": serde_json::json!({}) }
        ]
    });
   
    let parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config)).unwrap().unwrap();

    let endpoint = create_endpoint_with_one_address("127.0.0.1:1234".to_string());
    let pickfirst_endpoint = create_endpoint_with_one_address("0.0.0.0.0".to_string());
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint.clone()]),
        ..Default::default()
    };

    // Switch to first round_robin (current)
    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );

    
    let current_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    move_subchannel_to_ready(&mut *graceful_switch, current_subchannels[0].clone(), tcc.as_mut());
    verify_roundrobin_ready_picker_from_policy(&mut rx_events, current_subchannels[0].clone()).await;

    // 2. Switch to another round_robin (pending)
    let new_service_config = serde_json::json!({
        "children_policies": [
            { "pick_first": serde_json::json!({ "shuffleAddressList": false }) },
           
        ]
    });
    let pickfirst_update = ResolverUpdate {
        endpoints: Ok(vec![pickfirst_endpoint.clone()]),
        ..Default::default()
    };
    let new_parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(new_service_config)).unwrap().unwrap();
    graceful_switch
        .resolver_update(pickfirst_update.clone(), Some(&new_parsed_config), &mut *tcc)
        .unwrap();

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        Some("pick_first"),
        "Pending policy should be None after first update"
    );
    let pending_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    move_subchannel_to_idle(&mut *graceful_switch, pending_subchannels[0].clone(), tcc.as_mut());
    move_subchannel_to_connecting(&mut *graceful_switch, current_subchannels[0].clone(), tcc.as_mut());
}

#[tokio::test]
async fn gracefulswitch_current_leaving_ready() {
    let (mut rx_events, mut graceful_switch, mut tcc) = setup();

    // 1. Initial switch to round_robin (acts as mockBalancerBuilder1)
    let service_config = serde_json::json!({
        "children_policies": [
            { "round_robin": serde_json::json!({}) }
        ]
    });
   
    let parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config)).unwrap().unwrap();

    let endpoint = create_endpoint_with_one_address("127.0.0.1:1234".to_string());
    let pickfirst_endpoint = create_endpoint_with_one_address("0.0.0.0.0".to_string());
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint.clone()]),
        ..Default::default()
    };

    // Switch to first round_robin (current)
    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );

    
    let current_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    move_subchannel_to_ready(&mut *graceful_switch, current_subchannels[0].clone(), tcc.as_mut());
    verify_roundrobin_ready_picker_from_policy(&mut rx_events, current_subchannels[0].clone()).await;

    // 2. Switch to another round_robin (pending)
    let new_service_config = serde_json::json!({
        "children_policies": [
            { "pick_first": serde_json::json!({ "shuffleAddressList": false }) },
            
        ]
    });
    let pickfirst_update = ResolverUpdate {
        endpoints: Ok(vec![pickfirst_endpoint.clone()]),
        ..Default::default()
    };
    let new_parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(new_service_config)).unwrap().unwrap();
    graceful_switch
        .resolver_update(pickfirst_update.clone(), Some(&new_parsed_config), &mut *tcc)
        .unwrap();

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        Some("pick_first"),
        "Pending policy should be None after first update"
    );
    
    let pending_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    send_initial_subchannel_updates_to_policy(&mut *graceful_switch, &pending_subchannels, tcc.as_mut());
    verify_connection_attempt_from_policy(&mut rx_events, pending_subchannels[0].clone()).await;
   
    move_subchannel_to_connecting(&mut *graceful_switch, current_subchannels[0].clone(), tcc.as_mut());
 
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("pick_first"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        None,
        "Pending policy should be None after first update"
    );
    move_subchannel_to_ready(&mut *graceful_switch, pending_subchannels[0].clone(), tcc.as_mut());
    // let picker = verify_
    
}

#[tokio::test]
async fn gracefulswitch_pending_replaced_by_another_pending() {
    let (mut rx_events, mut graceful_switch, mut tcc) = setup();
    let service_config = serde_json::json!({
        "children_policies": [
            { "round_robin": serde_json::json!({}) }
        ]
    });
   
    let parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config)).unwrap().unwrap();

    let endpoint = create_endpoint_with_one_address("127.0.0.1:1234".to_string());
    let pickfirst_endpoint = create_endpoint_with_one_address("0.0.0.0.0".to_string());
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint.clone()]),
        ..Default::default()
    };

    // Switch to first round_robin (current)
    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );

    
    let current_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    move_subchannel_to_ready(&mut *graceful_switch, current_subchannels[0].clone(), tcc.as_mut());
    verify_roundrobin_ready_picker_from_policy(&mut rx_events, current_subchannels[0].clone()).await;

    // 2. Switch to another round_robin (pending)
    let new_service_config = serde_json::json!({
        "children_policies": [
            { "pick_first": serde_json::json!({ "shuffleAddressList": false }) },
            
        ]
    });
    let pickfirst_update = ResolverUpdate {
        endpoints: Ok(vec![pickfirst_endpoint.clone()]),
        ..Default::default()
    };
    let new_parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(new_service_config)).unwrap().unwrap();
    graceful_switch
        .resolver_update(pickfirst_update.clone(), Some(&new_parsed_config), &mut *tcc)
        .unwrap();

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        Some("pick_first"),
        "Pending policy should be None after first update"
    );

    let new_service_config = serde_json::json!({
        "children_policies": [
            { "mock_policy_one": serde_json::json!({ "shuffleAddressList": false }) },
            
        ]
    });
    let new_parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(new_service_config)).unwrap().unwrap();
    graceful_switch
        .resolver_update(pickfirst_update.clone(), Some(&new_parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        Some("mock_policy_one"),
        "Pending policy should be None after first update"
    );
}

#[tokio::test]
async fn gracefulswitch_updating_subchannel_removed_child() {
    let (mut rx_events, mut graceful_switch, mut tcc) = setup();
    let service_config = serde_json::json!({
        "children_policies": [
            { "round_robin": serde_json::json!({}) }
        ]
    });
   
    let parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(service_config)).unwrap().unwrap();

    let endpoint = create_endpoint_with_one_address("127.0.0.1:1234".to_string());
    let pickfirst_endpoint = create_endpoint_with_one_address("0.0.0.0.0".to_string());
    let update = ResolverUpdate {
        endpoints: Ok(vec![endpoint.clone()]),
        ..Default::default()
    };

    // Switch to first round_robin (current)
    graceful_switch
        .resolver_update(update.clone(), Some(&parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name(),
        None,
        "Pending policy should be None after first update"
    );

    
    let current_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
    move_subchannel_to_ready(&mut *graceful_switch, current_subchannels[0].clone(), tcc.as_mut());
    verify_roundrobin_ready_picker_from_policy(&mut rx_events, current_subchannels[0].clone()).await;

    // 2. Switch to another round_robin (pending)
    let new_service_config = serde_json::json!({
        "children_policies": [
            { "pick_first": serde_json::json!({ "shuffleAddressList": false }) },
            
        ]
    });
    let pickfirst_update = ResolverUpdate {
        endpoints: Ok(vec![pickfirst_endpoint.clone()]),
        ..Default::default()
    };
    let new_parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(new_service_config)).unwrap().unwrap();
    graceful_switch
        .resolver_update(pickfirst_update.clone(), Some(&new_parsed_config), &mut *tcc)
        .unwrap();

    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        Some("pick_first"),
        "Pending policy should be None after first update"
    );
    let pick_first_subchannels = verify_subchannel_creation_from_policy(&mut rx_events, pickfirst_endpoint.addresses).await;
    // send_initial_subchannel_updates_to_policy(&mut *graceful_switch, pick_first_subchannels[0].clone(), tcc.as_mut());

    let new_service_config = serde_json::json!({
        "children_policies": [
            { "mock_policy_one": serde_json::json!({ "shuffleAddressList": false }) },
            
        ]
    });
    let new_parsed_config = GracefulSwitchPolicy::parse_config(&ParsedJsonLbConfig(new_service_config)).unwrap().unwrap();
    graceful_switch
        .resolver_update(pickfirst_update.clone(), Some(&new_parsed_config), &mut *tcc)
        .unwrap();
    assert_eq!(
        graceful_switch.current_policy_name().as_deref(),
        Some("round_robin"),
        "Current policy should be round_robin after first update"
    );
    assert_eq!(
        graceful_switch.pending_policy_name().as_deref(),
        Some("mock_policy_one"),
        "Pending policy should be None after first update"
    );
    move_subchannel_to_ready(&mut *graceful_switch, pick_first_subchannels[0].clone(), tcc.as_mut());
    // let result = panic::catch_unwind(|| {
    //     move_subchannel_to_ready(&mut *graceful_switch, subchannel.clone(), tcc.as_mut());
    // });

    // assert!(result.is_err(), "Expected panic when moving subchannel to ready, but it did not panic");
}