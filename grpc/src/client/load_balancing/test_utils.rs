use crate::client::{
    load_balancing::{
        ChannelController, ExternalSubchannel, ForwardingSubchannel, LbPolicy, LbPolicyBuilder, LbPolicyOptions, LbState, ParsedJsonLbConfig, Pick, PickResult, Picker, Subchannel, SubchannelState, WorkScheduler
    }, name_resolution::Address, service_config::LbConfig, ConnectivityState
};
use crate::service::{Message, Request, Response, Service};
use std::{
    collections::HashMap, error::Error, fmt::Display, hash::{Hash, Hasher}, ops::Add, ptr, sync::Arc
};
use futures_util::future::ok;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, Notify},
    task::AbortHandle,
};
use tonic::metadata::MetadataMap;

pub(crate) struct EmptyMessage {}
impl Message for EmptyMessage {}
pub(crate) fn new_request() -> Request {
    Request::new(Box::pin(tokio_stream::once(
        Box::new(EmptyMessage {}) as Box<dyn Message>
    )))
}

pub(crate) struct TestSubchannel {
    address: Address,
    tx_connect: mpsc::UnboundedSender<TestEvent>,
}

impl TestSubchannel {
    fn new(address: Address, tx_connect: mpsc::UnboundedSender<TestEvent>) -> Self {
        Self {
            address,
            tx_connect,
        }
    }
}

impl ForwardingSubchannel for TestSubchannel {
    fn delegate(&self) -> Arc<dyn Subchannel> {
        panic!("unsupported operation on a test subchannel");
    }

    fn address(&self) -> Address {
        self.address.clone()
    }

    fn connect(&self) {
        println!("connect called for subchannel {}", self.address);
        self.tx_connect
            .send(TestEvent::Connect(self.address.clone()))
            .unwrap();
    }
}

impl Hash for TestSubchannel {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        ptr::hash(self, state); 
    }
}

impl PartialEq for TestSubchannel {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self, other)
    }
}
impl Eq for TestSubchannel {}

pub(crate) enum TestEvent {
    NewSubchannel(Address, Arc<dyn Subchannel>),
    UpdatePicker(LbState),
    RequestResolution,
    Connect(Address),
    ScheduleWork,
}

impl Display for TestEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewSubchannel(addr, _) => write!(f, "NewSubchannel({})", addr),
            Self::UpdatePicker(state) => write!(f, "UpdatePicker({})", state.connectivity_state),
            Self::RequestResolution => write!(f, "RequestResolution"),
            Self::Connect(addr) => write!(f, "Connect({})", addr.address),
            Self::ScheduleWork => write!(f, "ScheduleWork"),
        }
    }
}

//perhaps add stub balancer
pub(crate) struct FakeChannel {
    pub tx_events: mpsc::UnboundedSender<TestEvent>,
}

impl ChannelController for FakeChannel {
    fn new_subchannel(&mut self, address: &Address) -> Arc<dyn Subchannel> {
        println!("new_subchannel called for address {}", address);
        let notify = Arc::new(Notify::new());
        let subchannel: Arc<dyn Subchannel> =
            Arc::new(TestSubchannel::new(address.clone(), self.tx_events.clone()));
        self.tx_events
            .send(TestEvent::NewSubchannel(
                address.clone(),
                subchannel.clone(),
            ))
            .unwrap();
        subchannel
    }
    fn update_picker(&mut self, update: LbState) {
        println!("picker_update called with {}", update.connectivity_state);
        self.tx_events
            .send(TestEvent::UpdatePicker(update))
            .unwrap();
    }
    fn request_resolution(&mut self) {
        self.tx_events.send(TestEvent::RequestResolution).unwrap();
    }
}

pub(crate) struct TestWorkScheduler {
    pub tx_events: mpsc::UnboundedSender<TestEvent>,
}

impl WorkScheduler for TestWorkScheduler {
    fn schedule_work(&self) {
        self.tx_events.send(TestEvent::ScheduleWork).unwrap();
    }
}

pub struct MockBalancerOne {
    connectivity_state: ConnectivityState,
    subchannel_list: Option<SubchannelList>,
}

pub struct MockBalancerTwo {
    connectivity_state: ConnectivityState,
    subchannel_list: Option<SubchannelList>,
}

impl LbPolicy for MockBalancerOne {
    fn resolver_update(
        &mut self,
        update: crate::client::name_resolution::ResolverUpdate,
        config: Option<&crate::client::service_config::LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Ok(ref endpoints) = update.endpoints {
            let addresses: Vec<_> = endpoints.iter().flat_map(|ep| ep.addresses.clone()).collect();
            let scl = SubchannelList::new(&addresses, channel_controller);
            self.subchannel_list = Some(scl);
        }
        channel_controller.update_picker(LbState {
            connectivity_state: self.connectivity_state,
            picker: Arc::new(MockPickerOne),
        });
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &super::SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        if let Some(ref mut scl) = self.subchannel_list {
            scl.update_subchannel_data(&subchannel, state);
            println!("updating ready picker for mock balancer 2");
            // Optionally, send a picker update to simulate state change
            channel_controller.update_picker(LbState {
                connectivity_state: state.connectivity_state,
                picker: Arc::new(MockPickerOne), // or MockPickerTwo
            });
        }
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        todo!()
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        todo!()
    }
}

impl LbPolicy for MockBalancerTwo {
    fn resolver_update(
        &mut self,
        update: crate::client::name_resolution::ResolverUpdate,
        config: Option<&crate::client::service_config::LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Ok(ref endpoints) = update.endpoints {
            let addresses: Vec<_> = endpoints.iter().flat_map(|ep| ep.addresses.clone()).collect();
            let scl = SubchannelList::new(&addresses, channel_controller);
            self.subchannel_list = Some(scl);
        }
        channel_controller.update_picker(LbState {
            connectivity_state: self.connectivity_state,
            picker: Arc::new(MockPickerTwo),
        });
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &super::SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        if let Some(ref mut scl) = self.subchannel_list {
            // scl.update_subchannel_data(&subchannel, state);
            println!("updating ready picker for mock balancer 2");
            // Optionally, send a picker update to simulate state change
            channel_controller.update_picker(LbState {
                connectivity_state: state.connectivity_state,
                picker: Arc::new(MockPickerTwo), // or MockPickerTwo
            });
        }
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        todo!()
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        todo!()
    }
}
pub static POLICY_NAME: &str = "mock_policy_one";
pub static MOCK_POLICY_TWO: &str = "mock_policy_two";

struct MockPolicyOneBuilder {}
struct MockPolicyTwoBuilder {}


impl LbPolicyBuilder for MockPolicyOneBuilder {
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicy> {
        Box::new(MockBalancerOne {
            // work_scheduler: options.work_scheduler,
            subchannel_list: None,
            // selected_subchannel: None,
            // addresses: vec![],
            // last_resolver_error: None,
            // last_connection_error: None,
            connectivity_state: ConnectivityState::Connecting,
            // sent_connecting_state: false,
            // num_transient_failures: 0,
        })
    }

    fn name(&self) -> &'static str {
        POLICY_NAME
    }

    fn parse_config(
        &self,
        config: &ParsedJsonLbConfig,
    ) -> Result<Option<LbConfig>, Box<dyn Error + Send + Sync>> {
        let cfg: MockConfig = match config.convert_to() {
            Ok(c) => c,
            Err(e) => {
                return Err(format!("failed to parse JSON config: {}", e).into());
            }
        };
        Ok(Some(LbConfig::new(cfg)))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct MockConfig {
    shuffle_address_list: Option<bool>,
}

impl LbPolicyBuilder for MockPolicyTwoBuilder {
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicy> {
        Box::new(MockBalancerTwo {
            // work_scheduler: options.work_scheduler,
            subchannel_list: None,
            // selected_subchannel: None,
            // addresses: vec![],
            // last_resolver_error: None,
            // last_connection_error: None,
            connectivity_state: ConnectivityState::Connecting,
            // sent_connecting_state: false,
            // num_transient_failures: 0,
        })
    }

    fn name(&self) -> &'static str {
        MOCK_POLICY_TWO
    }

    fn parse_config(
        &self,
        config: &ParsedJsonLbConfig,
    ) -> Result<Option<LbConfig>, Box<dyn Error + Send + Sync>> {
        let cfg: MockConfig = match config.convert_to() {
            Ok(c) => c,
            Err(e) => {
                return Err(format!("failed to parse JSON config: {}", e).into());
            }
        };
        Ok(Some(LbConfig::new(cfg)))
    }

   
}

#[derive(Clone)]
struct SubchannelData {
    state: Option<SubchannelState>,
    seen_transient_failure: bool,
}

impl SubchannelData {
    fn new() -> SubchannelData {
        SubchannelData {
            state: None,
            seen_transient_failure: false,
        }
    }
}

struct SubchannelList {
    subchannels: HashMap<Arc<dyn Subchannel>, SubchannelData>,
    ordered_subchannels: Vec<Arc<dyn Subchannel>>,
    current_idx: usize,
    num_initial_notifications_seen: usize,
}

impl SubchannelList {
    fn new(addresses: &Vec<Address>, channel_controller: &mut dyn ChannelController) -> Self {
        let mut scl = SubchannelList {
            subchannels: HashMap::new(),
            ordered_subchannels: Vec::new(),
            current_idx: 0,
            num_initial_notifications_seen: 0,
        };
        for address in addresses {
            let sc = channel_controller.new_subchannel(address);
            scl.ordered_subchannels.push(sc.clone());
            scl.subchannels.insert(sc, SubchannelData::new());
        }

        println!("created new subchannel list with {} subchannels", scl.len());
        scl
    }

    fn len(&self) -> usize {
        self.ordered_subchannels.len()
    }

    fn subchannel_data(&self, sc: &Arc<dyn Subchannel>) -> Option<SubchannelData> {
        self.subchannels.get(sc).cloned()
    }

    fn contains(&self, sc: &Arc<dyn Subchannel>) -> bool {
        self.subchannels.contains_key(sc)
    }

    // Updates internal state of the subchannel with the new state. Callers must
    // ensure that this method is called only for subchannels in the list.
    //
    // Returns old state corresponding to the subchannel, if one exists.
    fn update_subchannel_data(
        &mut self,
        sc: &Arc<dyn Subchannel>,
        state: &SubchannelState,
    ) -> Option<SubchannelState> {
        let sc_data = self.subchannels.get_mut(sc).unwrap();

        // Increment the counter when seeing the first update.
        if sc_data.state.is_none() {
            self.num_initial_notifications_seen += 1;
        }

        let old_state = sc_data.state.clone();
        sc_data.state = Some(state.clone());
        match state.connectivity_state {
            ConnectivityState::Ready => sc_data.seen_transient_failure = false,
            ConnectivityState::TransientFailure => sc_data.seen_transient_failure = true,
            _ => {}
        }

        old_state
    }


    // Initiates a connection attempt to the next subchannel in the list that is
    // IDLE. Returns false if there are no more subchannels in the list.
    fn connect_to_next_subchannel(
        &mut self,
        channel_controller: &mut dyn ChannelController,
    ) -> bool {
        // Special case for the first connection attempt, as current_idx is set
        // to 0 when the subchannel list is created.
        if self.current_idx != 0 {
            self.current_idx += 1;
        }

        for idx in self.current_idx..self.ordered_subchannels.len() {
            // Grab the next subchannel and its data.
            let sc = &self.ordered_subchannels[idx];
            let sc_data = self.subchannels.get(sc).unwrap();

            match &sc_data.state {
                Some(state) => {
                    if state.connectivity_state == ConnectivityState::Connecting
                        || state.connectivity_state == ConnectivityState::TransientFailure
                    {
                        self.current_idx += 1;
                        continue;
                    } else if state.connectivity_state == ConnectivityState::Idle {
                        sc.connect();
                        return true;
                    }
                }
                None => {
                    debug_assert!(
                        false,
                        "No state available when asked to connect to subchannel: {}",
                        sc,
                    );
                }
            }
        }
        false
    }

}

pub struct MockPickerOne;
pub struct MockPickerTwo;

impl Picker for MockPickerOne {
    fn pick(&self, _req: &Request) -> PickResult {
        PickResult::Pick(Pick {
            subchannel: Arc::new(TestSubchannel::new(Address { address: "one".to_string(), ..Default::default() }, mpsc::unbounded_channel().0)),
            on_complete: None,
            metadata: MetadataMap::new(),
        })
    }
}

impl Picker for MockPickerTwo {
    fn pick(&self, _req: &Request) -> PickResult {
        PickResult::Pick(Pick {
            subchannel: Arc::new(TestSubchannel::new(Address { address: "two".to_string(), ..Default::default() }, mpsc::unbounded_channel().0)),
            on_complete: None,
            metadata: MetadataMap::new(),
        })
    }
}

pub fn reg() {
    super::GLOBAL_LB_REGISTRY.add_builder(MockPolicyOneBuilder {});
    super::GLOBAL_LB_REGISTRY.add_builder(MockPolicyTwoBuilder {});
}