use std::{
    collections::{HashMap, HashSet},
    error::Error,
    hash::Hash,
    ops::Sub,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::sleep;
use tonic::async_trait;
use tonic::metadata::MetadataMap;

use crate::{
    client::{
        load_balancing::{ErroringPicker, LbState, QueuingPicker},
        name_resolution::{Address, Endpoint, ResolverData, ResolverUpdate},
        subchannel, ConnectivityState,
    },
    service::{Request, Response, Service},
};

use super::{
    ChannelController, LbConfig, LbPolicyBuilderSingle, LbPolicyOptions, LbPolicySingle,
    ParsedJsonLbConfig, Pick, PickResult, Picker, Subchannel, SubchannelState, WorkScheduler,
};

use serde::{Deserialize, Serialize};
use serde_json::json;

use rand;
use rand::seq::SliceRandom;

pub static POLICY_NAME: &str = "pick_first";

struct Builder {}

impl LbPolicyBuilderSingle for Builder {
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicySingle> {
        Box::new(PickFirstPolicy {
            work_scheduler: options.work_scheduler,
            subchannel_list: None,
            selected_subchannel: None,
            addresses: vec![],
            last_resolver_error: None,
            last_connection_error: None,
            connectivity_state: ConnectivityState::Connecting,
            num_transient_failures: 0,
        })
    }

    fn name(&self) -> &'static str {
        POLICY_NAME
    }

    fn parse_config(
        &self,
        config: &ParsedJsonLbConfig,
    ) -> Result<Option<LbConfig>, Box<dyn Error + Send + Sync>> {
        let cfg: PickFirstConfig = match config.convert_to() {
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
struct PickFirstConfig {
    shuffle_address_list: Option<bool>,
}

pub fn reg() {
    super::GLOBAL_LB_REGISTRY.add_builder(Builder {})
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

struct PickFirstPolicy {
    work_scheduler: Arc<dyn WorkScheduler>, // Helps to schedule work.
    subchannel_list: Option<SubchannelList>, // List of subchannels, that we are currently connecting to.
    selected_subchannel: Option<Arc<Subchannel>>, // The currently selected subchannel.
    addresses: Vec<Address>,                 // Most recent addresses from the name resolver.
    last_resolver_error: Option<Box<dyn Error + Send + Sync>>, // Most recent error from the name resolver.
    last_connection_error: Option<Arc<dyn Error + Send + Sync>>, // Most recent error from any subchannel.
    connectivity_state: ConnectivityState, // Overall connectivity state of the channel.
    num_transient_failures: usize, // Number of transient failures after the end of the first pass.
}

impl LbPolicySingle for PickFirstPolicy {
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match update {
            ResolverUpdate::Data(data) => {
                println!("received update from resolver with data: {:?}", data);
                println!("received update from resolver with config: {:?}", config);

                // Shuffle endpoints if requested.
                let mut endpoints = data.endpoints.clone();
                if let Some(err) = self.shuffle_endpoints(config, &mut endpoints) {
                    println!("failed to shuffle endpoints: {}", err);
                    return Err(err);
                }

                // Perform other address list handling as specified in A61.
                let new_addresses: Vec<Address> = self.address_list_from_endpoints(&endpoints);

                // Treat empty resolver updates identically to resolver errors
                // that occur before any valid update has been received.
                if new_addresses.is_empty() {
                    self.handle_empty_endpoints(channel_controller);
                    return Err("received empty address list from the name resolver".into());
                }

                // Start using the new address list unless in IDLE, in which
                // case, we rely on exit_idle() for the same.
                if self.connectivity_state != ConnectivityState::Idle {
                    self.subchannel_list =
                        Some(SubchannelList::new(&new_addresses, channel_controller));
                }
                self.addresses = new_addresses;
            }
            ResolverUpdate::Err(error) => {
                println!("received error from resolver: {}", error);
                self.last_resolver_error = Some(error);

                // Enter or stay in TF, if there is no good previous update from
                // the resolver, or if already in TF. Regardless, send a new
                // failing picker with the updated error information.
                if self.addresses.is_empty()
                    || self.connectivity_state == ConnectivityState::TransientFailure
                {
                    self.to_transient_failure(channel_controller);
                }

                // Continue using the previous good update, if one exists.
            }
        }
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        subchannel: Arc<Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        println!("received update for {}: {}", subchannel, state);

        // Handle the update for this subchannel, provided it's included in the
        // subchannel list (if the list exists).
        if let Some(subchannel_list) = &self.subchannel_list {
            if subchannel_list.contains(subchannel.clone()) {
                if state.connectivity_state == ConnectivityState::Ready {
                    self.to_ready(subchannel, channel_controller);
                } else {
                    self.update_tracked_subchannel(subchannel, state, channel_controller);
                }
                return;
            }
        }

        // Handle updates for the currently selected subchannel.
        if let Some(selected_sc) = &self.selected_subchannel {
            if *selected_sc == subchannel {
                // Any state change for the currently connected subchannel means
                // that we are no longer connected.
                self.to_idle(channel_controller);
                return;
            }
        }

        debug_assert!(
            false,
            "received update for unknown subchannel: {}",
            subchannel
        );
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        // Build a new subchannel list with the most recent addresses received
        // from the name resolver. This will start connecting from the first
        // address in the list.
        self.subchannel_list = Some(SubchannelList::new(&self.addresses, channel_controller));
    }
}

impl PickFirstPolicy {
    fn shuffle_endpoints(
        &self,
        config: Option<&LbConfig>,
        endpoints: &mut Vec<Endpoint>,
    ) -> Option<Box<dyn Error + Send + Sync>> {
        if config.is_none() {
            return None;
        }

        let cfg: Arc<PickFirstConfig> = match config.unwrap().convert_to() {
            Ok(cfg) => cfg,
            Err(e) => return Some(e),
        };
        println!("received update from resolver with config: {:?}", &cfg);

        let mut shuffle_addresses = false;
        if let Some(v) = cfg.shuffle_address_list {
            shuffle_addresses = v;
        }

        // Perform the optional shuffling described in A62. The shuffling will
        // change the order of the endpoints but will not touch the order of the
        // addresses within each endpoint - A61.
        if shuffle_addresses {
            let mut rng = rand::thread_rng();
            endpoints.shuffle(&mut rng);
        };
        None
    }

    fn address_list_from_endpoints(&self, endpoints: &Vec<Endpoint>) -> Vec<Address> {
        // Flatten the endpoints list by concatenating the ordered list of
        // addresses for each of the endpoints.
        let mut addresses: Vec<Address> = endpoints
            .clone()
            .into_iter()
            .map(|ep| ep.addresses)
            .into_iter()
            .flatten()
            .collect();

        // Remove duplicates.
        let mut uniques = HashSet::new();
        addresses.retain(|e| uniques.insert(e.clone()));

        // TODO(easwars): Implement address family interleaving as part of
        // the dualstack implementation.

        return addresses;
    }

    fn handle_empty_endpoints(&mut self, channel_controller: &mut dyn ChannelController) {
        self.subchannel_list = None;
        self.selected_subchannel = None;
        self.addresses = vec![];
        self.last_resolver_error =
            Some("received empty address list from the name resolver".into());
        self.to_transient_failure(channel_controller);
        channel_controller.request_resolution();
    }

    // Handles updates for subchannels currently in the subchannel list.
    fn update_tracked_subchannel(
        &mut self,
        sc: Arc<Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        let subchannel_list = self.subchannel_list.as_mut().unwrap();

        // Update subchannel data. Return early if not all subchannels have seen
        // their first state update.
        let old_state = subchannel_list.update_subchannel_data(sc.clone(), state);
        if !subchannel_list.all_subchannels_seen_initial_state() {
            return;
        }

        // We're here only if all subchannels have seen their first update.

        // Handle the last subchannel to report its initial state.
        if old_state.is_none() {
            if self.selected_subchannel.is_some() {
                // Close the selected subchannel and go IDLE because it is no
                // longer part of the most recent update from the resolver. We
                // handle subchannel state transitions to READY much earlier in
                // subchannel_update().
                self.to_idle(channel_controller);
            } else {
                // Start connecting from the first subchannel.
                if !subchannel_list.connect_to_next_subchannel(channel_controller) {
                    debug_assert!(false, "failed to initiate connection to first subchannel");
                }
            }
            return;
        }

        // Otherwise, handle the most recent subchannel state transition.
        match state.connectivity_state {
            ConnectivityState::Idle => {
                // Immediately connect to subchannels transitioning to IDLE,
                // once the first pass is complete.
                if subchannel_list.is_first_pass_complete() {
                    sc.connect();
                }
            }
            ConnectivityState::Connecting => {
                if self.connectivity_state != ConnectivityState::TransientFailure {
                    // TODO(easwars): Prevent duplicate picker updates when the
                    // policy is in CONNECTING.
                    self.to_connecting(channel_controller);
                }
            }
            ConnectivityState::TransientFailure => {
                self.last_connection_error = state.last_connection_error.clone();

                if !subchannel_list.is_first_pass_complete() {
                    // Connect to the next subchannel in the list.
                    if !subchannel_list.connect_to_next_subchannel(channel_controller) {
                        // TODO(easwars): Call go_transient_failure() instead.
                        // Currently, doing this fails the borrow checker.

                        // Move to TRANSIENT_FAILURE and attempt to connect to
                        // all subchannels once we get to the end of the list.
                        self.connectivity_state = ConnectivityState::TransientFailure;
                        let err = format!(
                            "last seen resolver error: {:?}, last seen connection error: {:?}",
                            self.last_resolver_error, self.last_connection_error,
                        );
                        channel_controller.update_picker(LbState {
                            connectivity_state: ConnectivityState::TransientFailure,
                            picker: Arc::new(ErroringPicker {
                                error: format!("{}", err),
                            }),
                        });
                        channel_controller.request_resolution();
                        subchannel_list.connect_to_all_subchannels(channel_controller);
                    }
                } else {
                    self.num_transient_failures += 1;
                    if self.num_transient_failures == subchannel_list.len() {
                        // Request re-resolution and update the error picker.
                        self.to_transient_failure(channel_controller);
                        self.num_transient_failures = 0;
                    }
                }
            }
            _ => {
                debug_assert!(
                    false,
                    "unexpected state transition for subchannel {}: {:?} -> {:?}",
                    sc,
                    old_state.unwrap().connectivity_state,
                    state.connectivity_state
                );
            }
        }
    }

    fn to_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        self.connectivity_state = ConnectivityState::Idle;
        self.subchannel_list = None;
        self.selected_subchannel = None;
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::Idle,
            picker: Arc::new(IdlePicker {
                work_scheduler: self.work_scheduler.clone(),
            }),
        });
        channel_controller.request_resolution();
    }

    fn to_connecting(&mut self, channel_controller: &mut dyn ChannelController) {
        self.connectivity_state = ConnectivityState::Connecting;
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::Connecting,
            picker: Arc::new(QueuingPicker {}),
        });
    }

    fn to_ready(&mut self, sc: Arc<Subchannel>, channel_controller: &mut dyn ChannelController) {
        self.connectivity_state = ConnectivityState::Ready;
        self.selected_subchannel = Some(sc.clone());
        self.subchannel_list = None;
        self.last_connection_error = None;
        self.last_resolver_error = None;
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::Ready,
            picker: Arc::new(OneSubchannelPicker { sc: sc.clone() }),
        });
    }

    fn to_transient_failure(&mut self, channel_controller: &mut dyn ChannelController) {
        self.connectivity_state = ConnectivityState::TransientFailure;
        let err = format!(
            "last seen resolver error: {:?}, last seen connection error: {:?}",
            self.last_resolver_error, self.last_connection_error,
        );
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::TransientFailure,
            picker: Arc::new(ErroringPicker {
                error: format!("{}", err),
            }),
        });
        channel_controller.request_resolution();
    }
}

struct OneSubchannelPicker {
    sc: Arc<Subchannel>,
}

impl Picker for OneSubchannelPicker {
    fn pick(&self, request: &Request) -> PickResult {
        PickResult::Pick(Pick {
            subchannel: self.sc.clone(),
            on_complete: None,
            metadata: MetadataMap::new(),
        })
    }
}

pub struct IdlePicker {
    work_scheduler: Arc<dyn WorkScheduler>,
}

impl Picker for IdlePicker {
    fn pick(&self, request: &Request) -> PickResult {
        self.work_scheduler.schedule_work();
        PickResult::Queue
    }
}

struct SubchannelList {
    subchannels: HashMap<Arc<Subchannel>, SubchannelData>,
    ordered_subchannels: Vec<Arc<Subchannel>>,
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
            scl.subchannels.insert(sc.clone(), SubchannelData::new());
        }

        println!("created new subchannel list with {} subchannels", scl.len());
        scl
    }

    fn len(&self) -> usize {
        self.ordered_subchannels.len()
    }

    fn subchannel_data(&self, sc: Arc<Subchannel>) -> Option<SubchannelData> {
        self.subchannels.get(&sc).cloned()
    }

    fn contains(&self, sc: Arc<Subchannel>) -> bool {
        self.subchannels.contains_key(&sc)
    }

    // Updates internal state of the subchannel with the new state. Callers must
    // ensure that this method is called only for subchannels in the list.
    //
    // Returns old state corresponding to the subchannel, if one exists.
    fn update_subchannel_data(
        &mut self,
        subchannel: Arc<Subchannel>,
        state: &SubchannelState,
    ) -> Option<SubchannelState> {
        let sc_data = self.subchannels.get_mut(&subchannel).unwrap();

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

        return old_state;
    }

    fn all_subchannels_seen_initial_state(&self) -> bool {
        self.num_initial_notifications_seen == self.ordered_subchannels.len()
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
            self.current_idx += 1;
        }
        return false;
    }

    fn is_first_pass_complete(&self) -> bool {
        if self.current_idx < self.ordered_subchannels.len() {
            return false;
        }
        for (_, data) in &self.subchannels {
            if !data.seen_transient_failure {
                return false;
            }
        }
        return true;
    }

    fn connect_to_all_subchannels(&mut self, channel_controller: &mut dyn ChannelController) {
        for (sc, data) in &mut self.subchannels {
            if data.state.as_ref().unwrap().connectivity_state == ConnectivityState::Idle {
                sc.connect();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::load_balancing::{
        LbConfig, LbPolicyBuilderSingle, SubchannelDropNotifier, GLOBAL_LB_REGISTRY,
    };
    use crate::client::subchannel::{InternalSubchannelPool, SubchannelImpl};
    use crate::client::transport::{Transport, GLOBAL_TRANSPORT_REGISTRY};
    use crate::service::{Message, Request, Response, Service};
    use std::io::Empty;
    use std::ops::Add;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::task::AbortHandle;

    struct EmptyMessage {}
    impl Message for EmptyMessage {}
    fn new_empty_request() -> Request {
        Request::new(Box::new(tokio_stream::once(
            Box::new(EmptyMessage {}) as Box<dyn Message>
        )))
    }
    fn new_empty_response() -> Response {
        Response::new(Box::new(tokio_stream::once(Ok(
            Box::new(EmptyMessage {}) as Box<dyn Message>
        ))))
    }

    #[test]
    fn pickfirst_builder_name() -> Result<(), String> {
        reg();

        let builder: Arc<dyn LbPolicyBuilderSingle> =
            match GLOBAL_LB_REGISTRY.get_policy("pick_first") {
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

        let builder: Arc<dyn LbPolicyBuilderSingle> =
            match GLOBAL_LB_REGISTRY.get_policy("pick_first") {
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
            assert_eq!(
                got_config.shuffle_address_list == tc.want_shuffle_addresses,
                true
            );
        }
        Ok(())
    }

    type TestWorkQueueItem =
        Box<dyn FnOnce(&mut TestChannelController, &mut Box<dyn LbPolicySingle>) + Send + Sync>;
    type WorkQueueTx = mpsc::UnboundedSender<TestWorkQueueItem>;

    struct TestHelper {
        rx_new_subchannel: mpsc::UnboundedReceiver<Address>,
        rx_subchannel: mpsc::UnboundedReceiver<Arc<Subchannel>>,
        rx_update_picker: mpsc::UnboundedReceiver<LbState>,
        rx_request_resolution: mpsc::UnboundedReceiver<()>,
        rx_connect: mpsc::UnboundedReceiver<Address>,
        wtx: WorkQueueTx,
        abort_handle: AbortHandle,
    }

    impl TestHelper {
        fn new(
            rx_new_subchannel: mpsc::UnboundedReceiver<Address>,
            rx_subchannel: mpsc::UnboundedReceiver<Arc<Subchannel>>,
            rx_update_picker: mpsc::UnboundedReceiver<LbState>,
            rx_request_resolution: mpsc::UnboundedReceiver<()>,
            rx_connect: mpsc::UnboundedReceiver<Address>,
            wtx: WorkQueueTx,
            abort_handle: AbortHandle,
        ) -> Self {
            Self {
                rx_new_subchannel,
                rx_subchannel,
                rx_update_picker,
                rx_request_resolution,
                rx_connect,
                wtx,
                abort_handle,
            }
        }

        fn send_resolver_update(&self, update: ResolverUpdate, config: Option<LbConfig>) {
            // let cfg = config.unwrap_or(LbConfig::new(ParsedJsonLbConfig(json!({}))));
            let _ = self.wtx.send(Box::new(
                move |tcc: &mut TestChannelController, lb_policy: &mut Box<dyn LbPolicySingle>| {
                    // let _ = lb_policy.resolver_update(update, Some(&cfg), tcc);
                    let _ = lb_policy.resolver_update(update, config.as_ref(), tcc);
                },
            ));
        }

        fn send_subchannel_update(&self, subchannel: Arc<Subchannel>, state: SubchannelState) {
            let _ = self.wtx.send(Box::new(
                move |tcc: &mut TestChannelController, lb_policy: &mut Box<dyn LbPolicySingle>| {
                    lb_policy.subchannel_update(subchannel, &state, tcc);
                },
            ));
            dbg!("sent subchannel update");
        }
    }

    impl Drop for TestHelper {
        fn drop(&mut self) {
            self.abort_handle.abort();
        }
    }

    struct TestChannelController {
        tx_new_subchannel: mpsc::UnboundedSender<Address>,
        tx_subchannel: mpsc::UnboundedSender<Arc<Subchannel>>,
        tx_update_picker: mpsc::UnboundedSender<LbState>,
        tx_request_resolution: mpsc::UnboundedSender<()>,
        tx_connect: mpsc::UnboundedSender<Address>,
    }

    impl TestChannelController {
        fn new(
            tx_new_subchannel: mpsc::UnboundedSender<Address>,
            tx_subchannel: mpsc::UnboundedSender<Arc<Subchannel>>,
            tx_update_picker: mpsc::UnboundedSender<LbState>,
            tx_request_resolution: mpsc::UnboundedSender<()>,
            tx_connect: mpsc::UnboundedSender<Address>,
        ) -> Self {
            Self {
                tx_new_subchannel,
                tx_subchannel,
                tx_update_picker,
                tx_request_resolution,
                tx_connect,
            }
        }
    }

    impl ChannelController for TestChannelController {
        fn new_subchannel(&mut self, address: &Address) -> Arc<Subchannel> {
            println!("new_subchannel called for address {}", address);
            self.tx_new_subchannel.send(address.clone()).unwrap();
            let subchannel = Arc::new(Subchannel::new(
                address.clone(),
                Arc::new(TestNopSubchannelDropNotifier {}),
                Arc::new(TestNopSubchannelImpl::new(
                    address.clone(),
                    self.tx_connect.clone(),
                )),
            ));
            self.tx_subchannel.send(subchannel.clone()).unwrap();
            subchannel
        }
        fn update_picker(&mut self, update: LbState) {
            self.tx_update_picker.send(update).unwrap();
        }
        fn request_resolution(&mut self) {
            self.tx_request_resolution.send(()).unwrap();
        }
    }

    struct TestWorkScheduler {
        wtx: WorkQueueTx,
    }

    impl WorkScheduler for TestWorkScheduler {
        fn schedule_work(&self) {
            let _ = self.wtx.send(Box::new(
                |tcc: &mut TestChannelController, lb: &mut Box<dyn LbPolicySingle>| {
                    lb.work(tcc);
                },
            ));
        }
    }

    struct TestNopSubchannelImpl {
        address: Address,
        tx_connect: mpsc::UnboundedSender<Address>,
    }

    impl TestNopSubchannelImpl {
        fn new(address: Address, tx_connect: mpsc::UnboundedSender<Address>) -> Self {
            Self {
                address,
                tx_connect,
            }
        }
    }

    struct TestNopSubchannelDropNotifier {}
    impl SubchannelDropNotifier for TestNopSubchannelDropNotifier {
        fn drop_subchannel(&self, address: &Address) {}
    }

    impl SubchannelImpl for TestNopSubchannelImpl {
        fn connect(&self, now: bool) {
            self.tx_connect.send(self.address.clone()).unwrap();
        }

        fn subchannel_state(&self) -> SubchannelState {
            SubchannelState {
                connectivity_state: ConnectivityState::Ready,
                last_connection_error: None,
            }
        }
    }
    #[async_trait]
    impl Service for TestNopSubchannelImpl {
        async fn call(&self, _method: String, request: Request) -> Response {
            new_empty_response()
        }
    }

    fn setup() -> TestHelper {
        // Register the pick_first LB policy builder.
        reg();

        let (tx_new_subchannel, rx_new_subchannel) = mpsc::unbounded_channel::<Address>();
        let (tx_subchannel, rx_subchannel) = mpsc::unbounded_channel::<Arc<Subchannel>>();
        let (tx_update_picker, rx_update_picker) = mpsc::unbounded_channel::<LbState>();
        let (tx_request_resolution, rx_request_resolution) = mpsc::unbounded_channel::<()>();
        let (tx_connect, rx_connect) = mpsc::unbounded_channel::<Address>();
        let (tx, mut rx) = mpsc::unbounded_channel::<TestWorkQueueItem>();
        let work_scheduler = Arc::new(TestWorkScheduler { wtx: tx.clone() });

        let builder: Arc<dyn LbPolicyBuilderSingle> =
            GLOBAL_LB_REGISTRY.get_policy("pick_first").unwrap();
        let mut lb_policy = builder.build(LbPolicyOptions {
            work_scheduler: work_scheduler,
        });

        let mut tcc = TestChannelController::new(
            tx_new_subchannel,
            tx_subchannel,
            tx_update_picker,
            tx_request_resolution,
            tx_connect,
        );
        // Spawn a task to process the work queue.
        let jh = tokio::task::spawn(async move {
            while let Some(w) = rx.recv().await {
                dbg!("processing work");
                w(&mut tcc, &mut lb_policy);
            }
            dbg!("work queue done");
        });

        TestHelper::new(
            rx_new_subchannel,
            rx_subchannel,
            rx_update_picker,
            rx_request_resolution,
            rx_connect,
            tx.clone(),
            jh.abort_handle(),
        )
    }

    #[tokio::test]
    async fn pickfirst_connects_to_first_address() {
        // Build the pick_first LB policy.
        let mut test_helper = setup();

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
        test_helper.send_resolver_update(update, None);

        // Verify that subchannels are created for the above addresses.
        assert!(test_helper.rx_new_subchannel.recv().await.unwrap() == address1);
        assert!(test_helper.rx_new_subchannel.recv().await.unwrap() == address2);

        // Send initial state of IDLE for the above subchannels.
        let subchannel1 = test_helper.rx_subchannel.recv().await.unwrap();
        test_helper.send_subchannel_update(subchannel1.clone(), SubchannelState::default());
        let subchannel2 = test_helper.rx_subchannel.recv().await.unwrap();
        test_helper.send_subchannel_update(subchannel2.clone(), SubchannelState::default());

        // Ensure that a connection is attempted to the first subchannel.
        assert!(test_helper.rx_connect.recv().await.unwrap() == address1);

        // Move first subchannel to CONNECTING.
        test_helper.send_subchannel_update(
            subchannel1.clone(),
            SubchannelState {
                connectivity_state: ConnectivityState::Connecting,
                ..Default::default()
            },
        );

        // Ensure that the channel becomes CONNECTING, with a queuing picker.
        let picker_update = test_helper.rx_update_picker.recv().await.unwrap();
        assert!(picker_update.connectivity_state == ConnectivityState::Connecting);
        let req = new_empty_request();
        assert!(picker_update.picker.pick(&req) == PickResult::Queue);

        // Move first subchannel to READY.
        test_helper.send_subchannel_update(
            subchannel1.clone(),
            SubchannelState {
                connectivity_state: ConnectivityState::Ready,
                ..Default::default()
            },
        );

        // Ensure that the channel becomes READY, with a ready picker.
        let picker_update = test_helper.rx_update_picker.recv().await.unwrap();
        assert!(picker_update.connectivity_state == ConnectivityState::Ready);
        let req = new_empty_request();
        let result = picker_update.picker.pick(&req);
        match result {
            PickResult::Pick(pick) => {
                assert!(pick.subchannel == subchannel1);
            }
            _ => panic!("unexpected pick result"),
        }

        // Move first subchannel to Idle.
        test_helper.send_subchannel_update(
            subchannel1.clone(),
            SubchannelState {
                connectivity_state: ConnectivityState::Idle,
                ..Default::default()
            },
        );

        // Ensure that the channel moves to IDLE.
        let picker_update = test_helper.rx_update_picker.recv().await.unwrap();
        assert!(picker_update.connectivity_state == ConnectivityState::Idle);
    }
}
