#[cfg(test)]
use super::*;
use crate::client::{
    load_balancing::{
        ChannelController, ErroringPicker, LbConfig, LbPolicyBuilder, LbState, QueuingPicker,
        Subchannel, SubchannelImpl, WorkScheduler, GLOBAL_LB_REGISTRY,
    },
    name_resolution::{Address, Endpoint, ResolverData, ResolverUpdate},
    subchannel::{ConnectivityStateWatcher, InternalSubchannel, InternalSubchannelPool},
    transport::{Transport, GLOBAL_TRANSPORT_REGISTRY},
    ConnectivityState,
};
use crate::service::{Message, Request, Response, Service};
use std::{ops::Add, sync::Arc};
use tokio::{
    sync::{mpsc, Notify},
    task::AbortHandle,
};
use tonic::async_trait;

struct EmptyMessage {}
impl Message for EmptyMessage {}
fn new_request() -> Request {
    Request::new(Box::pin(tokio_stream::once(
        Box::new(EmptyMessage {}) as Box<dyn Message>
    )))
}
fn new_response() -> Response {
    Response::new(Box::pin(tokio_stream::once(Ok(
        Box::new(EmptyMessage {}) as Box<dyn Message>
    ))))
}

#[test]
fn pickfirst_builder_name() -> Result<(), String> {
    reg();

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
    reg();

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

struct TestNopSubchannelImpl {
    address: Address,
    tx_connect: mpsc::UnboundedSender<TestEvent>,
}

impl TestNopSubchannelImpl {
    fn new(address: Address, tx_connect: mpsc::UnboundedSender<TestEvent>) -> Self {
        Self {
            address,
            tx_connect,
        }
    }
}

impl InternalSubchannel for TestNopSubchannelImpl {
    fn connect(&self, now: bool) {
        self.tx_connect
            .send(TestEvent::Connect(self.address.clone()))
            .unwrap();
    }

    fn register_connectivity_state_watcher(&self, watcher: Arc<dyn ConnectivityStateWatcher>) {}

    fn unregister_connectivity_state_watcher(&self, watcher: Arc<dyn ConnectivityStateWatcher>) {}
}

#[async_trait]
impl Service for TestNopSubchannelImpl {
    async fn call(&self, method: String, request: Request) -> Response {
        new_response()
    }
}

enum TestEvent {
    NewSubchannel(Address, Arc<dyn Subchannel>),
    UpdatePicker(LbState),
    RequestResolution,
    Connect(Address),
    ScheduleWork,
}

struct FakeChannel {
    tx_events: mpsc::UnboundedSender<TestEvent>,
}

impl ChannelController for FakeChannel {
    fn new_subchannel(&mut self, address: &Address) -> Arc<dyn Subchannel> {
        println!("new_subchannel called for address {}", address);
        let notify = Arc::new(Notify::new());
        let subchannel: Arc<dyn Subchannel> = Arc::new(SubchannelImpl::new(
            address.clone(),
            notify.clone(),
            Arc::new(TestNopSubchannelImpl::new(
                address.clone(),
                self.tx_events.clone(),
            )),
        ));
        self.tx_events
            .send(TestEvent::NewSubchannel(
                address.clone(),
                subchannel.clone(),
            ))
            .unwrap();
        subchannel
    }
    fn update_picker(&mut self, update: LbState) {
        self.tx_events
            .send(TestEvent::UpdatePicker(update))
            .unwrap();
    }
    fn request_resolution(&mut self) {
        self.tx_events.send(TestEvent::RequestResolution).unwrap();
    }
}

struct TestWorkScheduler {
    tx_events: mpsc::UnboundedSender<TestEvent>,
}

impl WorkScheduler for TestWorkScheduler {
    fn schedule_work(&self) {
        self.tx_events.send(TestEvent::ScheduleWork).unwrap();
    }
}

#[tokio::test]
async fn pickfirst_connects_to_first_address() {
    // Setup the test environment.
    reg();
    let (tx_events, mut rx_events) = mpsc::unbounded_channel::<TestEvent>();
    let work_scheduler = Arc::new(TestWorkScheduler {
        tx_events: tx_events.clone(),
    });
    let mut tcc = FakeChannel {
        tx_events: tx_events.clone(),
    };

    // Build the pick_first LB policy.
    let builder: Arc<dyn LbPolicyBuilder> = GLOBAL_LB_REGISTRY.get_policy("pick_first").unwrap();
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
    let req = new_request();
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
    let req = new_request();
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
