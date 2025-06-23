
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::client::{load_balancing::{graceful_switch::{self, GracefulSwitchPolicy}, pick_first, round_robin, test_utils::{FakeChannel, TestEvent, TestWorkScheduler}, ChannelController, LbPolicy, LbPolicyBuilder, LbPolicyOptions, ParsedJsonLbConfig, GLOBAL_LB_REGISTRY}, name_resolution::{Address, Endpoint, ResolverUpdate}, service_config::ServiceConfig};



fn setup() -> (
    mpsc::UnboundedReceiver<TestEvent>,
    Box<dyn LbPolicy>,
    // Box<dyn LbPolicy>,
    // Box<dyn LbPolicy>,
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

#[test]
fn gracefulswitch_test_switch_to_new_balancer() {
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
        endpoints: Ok(vec![endpoint]),
        ..Default::default()
    };
    graceful_switch
        .resolver_update(update, Some(&parsed_config), &mut *tcc)
        .unwrap();
    
}

// #[test]
// fn test_switch_multiple_times() {
//     let balancer1 = Arc::new(DummyBalancer::new());
//     let balancer2 = Arc::new(DummyBalancer::new());
//     let balancer3 = Arc::new(DummyBalancer::new());

//     let mut switch = GracefulSwitch::new(balancer1.clone());
//     assert!(switch.pick().is_err());
//     assert!(*balancer1.called.lock().unwrap());

//     switch.switch_to(balancer2.clone());
//     assert!(switch.pick().is_err());
//     assert!(*balancer2.called.lock().unwrap());

//     switch.switch_to(balancer3.clone());
//     assert!(switch.pick().is_err());
//     assert!(*balancer3.called.lock().unwrap());
// }

// #[test]
// fn test_no_switch_uses_initial_balancer() {
//     let balancer = Arc::new(DummyBalancer::new());
//     let mut switch = GracefulSwitch::new(balancer.clone());
//     assert!(switch.pick().is_err());
//     assert!(*balancer.called.lock().unwrap());
// }