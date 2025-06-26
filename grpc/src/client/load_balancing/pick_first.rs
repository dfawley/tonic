use std::{
    collections::{HashMap, HashSet},
    error::Error,
    hash::Hash,
    ops::Sub,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use crate::{
    client::{
        load_balancing::{
            ChannelController, Failing, LbPolicy, LbPolicyBuilder, LbPolicyOptions, LbState,
            ParsedJsonLbConfig, Pick, PickResult, Picker, QueuingPicker, Subchannel,
            ExternalSubchannel, SubchannelState, WorkScheduler,
        },
        name_resolution::{Address, Endpoint, ResolverUpdate},
        service_config::LbConfig,
        subchannel, ConnectivityState,
    },
    service::{Request, Response, Service},
};

use once_cell::sync::Lazy;
use rand::{self, rngs::StdRng, seq::SliceRandom, thread_rng, Rng, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::time::sleep;
use tonic::{async_trait, metadata::MetadataMap};

type EndpointShuffler = dyn Fn(&mut [Endpoint]) + Send + Sync + 'static;
pub static SHUFFLE_ENDPOINTS_FN: LazyLock<Mutex<Box<EndpointShuffler>>> =
    std::sync::LazyLock::new(|| {
        let shuffle_endpoints: Box<EndpointShuffler> = Box::new(|endpoints: &mut [Endpoint]| {
            let mut rng = thread_rng();
            endpoints.shuffle(&mut rng);
        });
        Mutex::new(shuffle_endpoints)
    });
pub(crate) fn thread_rng_shuffler() -> Box<EndpointShuffler> {
    Box::new(|endpoints: &mut [Endpoint]| {
        let mut rng = thread_rng();
        endpoints.shuffle(&mut rng);
    })
}

#[cfg(test)]
mod test;

pub static POLICY_NAME: &str = "pick_first";

struct Builder {}

impl LbPolicyBuilder for Builder {
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicy> {
        Box::new(PickFirstPolicy {
            work_scheduler: options.work_scheduler,
            subchannel_list: None,
            selected_subchannel: None,
            addresses: vec![],
            last_resolver_error: None,
            last_connection_error: None,
            connectivity_state: ConnectivityState::Connecting,
            sent_connecting_state: false,
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
pub(super) struct PickFirstConfig {
    shuffle_address_list: Option<bool>,
}

pub fn reg() {
    super::GLOBAL_LB_REGISTRY.add_builder(Builder {});
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
    selected_subchannel: Option<Arc<dyn Subchannel>>, // The currently selected subchannel.
    addresses: Vec<Address>,                 // Most recent addresses from the name resolver.
    last_resolver_error: Option<String>,     // Most recent error from the name resolver.
    last_connection_error: Option<Arc<dyn Error + Send + Sync>>, // Most recent error from any subchannel.
    connectivity_state: ConnectivityState, // Overall connectivity state of the channel.
    sent_connecting_state: bool, // Whether we have sent a CONNECTING state to the channel.
    num_transient_failures: usize, // Number of transient failures after the end of the first pass.
}

impl LbPolicy for PickFirstPolicy {
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match update.endpoints {
            Ok(mut endpoints) => {
                println!(
                    "received update from resolver with endpoints: {:?}",
                    endpoints
                );

                // Shuffle endpoints if requested.
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
            Err(error) => {
                println!("received error from resolver: {}", error);
                self.last_resolver_error = Some(error);

                // Enter or stay in TF, if there is no good previous update from
                // the resolver, or if already in TF. Regardless, send a new
                // failing picker with the updated error information.
                if self.addresses.is_empty()
                    || self.connectivity_state == ConnectivityState::TransientFailure
                {
                    self.move_to_transient_failure(channel_controller);
                }

                // Continue using the previous good update, if one exists.
            }
        }
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        subchannel: Arc<dyn Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        println!("received update for {}: {}", subchannel, state);

        // Handle the update for this subchannel, provided it's included in the
        // subchannel list (if the list exists).
        if let Some(subchannel_list) = &self.subchannel_list {
            if subchannel_list.contains(&subchannel) {
                if state.connectivity_state == ConnectivityState::Ready {
                    self.move_to_ready(subchannel, channel_controller);
                } else {
                    self.update_tracked_subchannel(subchannel, state, channel_controller);
                }
                return;
            }
        }
        
        // Handle updates for the currently selected subchannel.
        if let Some(selected_sc) = &self.selected_subchannel {
            if *selected_sc == subchannel.clone() {
                // Any state change for the currently connected subchannel means
                // that we are no longer connected.
                // if state.connectivity_state != ConnectivityState::Ready{
                println!("moving to idle");
            
                self.move_to_idle(channel_controller);
                return;
                }
            // }
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

    fn exit_idle(& mut self, channel_controller: &mut dyn ChannelController) {
        println!("child calls exit_idle");
        if self.connectivity_state == ConnectivityState::Idle {
            let prev_list = &self.subchannel_list;
            self.subchannel_list = Some(SubchannelList::new(&self.addresses, channel_controller));
            self.move_to_connecting(channel_controller);
            // if let Some(subchannel_list) = self.subchannel_list.as_mut() {
            // Initiate connection attempt to the first subchannel in the list
            if let Some(subchannel_list) = self.subchannel_list.as_mut() {
                subchannel_list.connect_after_idle();
            }
            // self.subcconnect_to_all_subchannels(channel_controller);
            // if let Some(subchannel_list) = self.subchannel_list.as_mut() {
            //     println!("connecting to all subchannels");
            //     let _ = subchannel_list.connect_to_next_subchannel(channel_controller);
            // }
        }
    }
}


fn shuffle_endpoints(endpoints: &mut [Endpoint]) {
    let mut rng = rand::thread_rng();
    endpoints.shuffle(&mut rng);
}

impl PickFirstPolicy {
    fn shuffle_endpoints(
        &self,
        config: Option<&LbConfig>,
        endpoints: &mut [Endpoint],
    ) -> Option<Box<dyn Error + Send + Sync>> {
        config?;
        // if config.is_none(){
        //     println!("is not none");
        //     return None
        // }

        println!("why are we here");
        let cfg: Arc<PickFirstConfig> = match config.unwrap().convert_to() {
            Ok(cfg) => cfg,
            Err(e) => return Some(e),
        };
        println!("received update from resolver with config: {:?}", &cfg);
        // let cfg: Arc<PickFirstConfig> = match config.unwrap().convert_to() {
        //     Ok(cfg) => cfg,
        //     Err(e) => return None,
        // };
        println!("received update from resolver with config: {:?}", &cfg);

        let mut shuffle_addresses = false;
        if let Some(v) = cfg.shuffle_address_list {
            shuffle_addresses = v;
        }

        // Perform the optional shuffling described in A62. The shuffling will
        // change the order of the endpoints but will not touch the order of the
        // addresses within each endpoint - A61.
        if shuffle_addresses {
            SHUFFLE_ENDPOINTS_FN.lock().unwrap()(endpoints);
        };
        None
    }

    fn address_list_from_endpoints(&self, endpoints: &[Endpoint]) -> Vec<Address> {
        // Flatten the endpoints list by concatenating the ordered list of
        // addresses for each of the endpoints.
        let mut addresses: Vec<Address> = endpoints
            .iter()
            .flat_map(|ep| ep.addresses.clone())
            .collect();

        // Remove duplicates.
        let mut uniques = HashSet::new();
        addresses.retain(|e| uniques.insert(e.clone()));

        // TODO(easwars): Implement address family interleaving as part of
        // the dualstack implementation.

        addresses
    }

    fn handle_empty_endpoints(&mut self, channel_controller: &mut dyn ChannelController) {
        self.subchannel_list = None;
        self.selected_subchannel = None;
        self.addresses = vec![];
        let res_err = String::from("received empty address list from the name resolver");
        self.last_resolver_error = Some(res_err);
        self.move_to_transient_failure(channel_controller);
        channel_controller.request_resolution();
    }

    // Handles updates for subchannels currently in the subchannel list.
    fn update_tracked_subchannel(
        &mut self,
        sc: Arc<dyn Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        let subchannel_list = self.subchannel_list.as_mut().unwrap();

        // Update subchannel data. Return early if not all subchannels have seen
        // their first state update.
        let old_state = subchannel_list.update_subchannel_data(&sc, state);
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
                self.move_to_idle(channel_controller);
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
                // If we are already in CONNECTING, ignore this update.
                if self.connectivity_state == ConnectivityState::Connecting
                    && self.sent_connecting_state
                {
                    return;
                }
                if self.connectivity_state != ConnectivityState::TransientFailure {
                    self.move_to_connecting(channel_controller);
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
                            picker: Arc::new(Failing { error: err }),
                        });
                        channel_controller.request_resolution();
                        println!("first pass complete, connecting to all subchannels");
                        subchannel_list.connect_to_all_subchannels(channel_controller);
                    }
                } else {
                    self.num_transient_failures += 1;
                    if self.num_transient_failures == subchannel_list.len() {
                        // Request re-resolution and update the error picker.
                        self.move_to_transient_failure(channel_controller);
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

    fn move_to_idle(&mut self, channel_controller: &mut dyn ChannelController) {
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
        self.sent_connecting_state = false;
    }

    fn move_to_connecting(&mut self, channel_controller: &mut dyn ChannelController) {
        self.connectivity_state = ConnectivityState::Connecting;
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::Connecting,
            picker: Arc::new(QueuingPicker {}),
        });
        self.sent_connecting_state = true;
    }

    fn move_to_ready(
        &mut self,
        sc: Arc<dyn Subchannel>,
        channel_controller: &mut dyn ChannelController,
    ) {
        self.connectivity_state = ConnectivityState::Ready;
        self.selected_subchannel = Some(sc.clone());
        self.subchannel_list = None;
        self.last_connection_error = None;
        self.last_resolver_error = None;
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::Ready,
            picker: Arc::new(OneSubchannelPicker { sc: sc.clone() }),
        });
        self.sent_connecting_state = false;
    }

    fn move_to_transient_failure(&mut self, channel_controller: &mut dyn ChannelController) {
        self.connectivity_state = ConnectivityState::TransientFailure;
        let err = format!(
            "last seen resolver error: {:?}, last seen connection error: {:?}",
            self.last_resolver_error, self.last_connection_error,
        );
        channel_controller.update_picker(LbState {
            connectivity_state: ConnectivityState::TransientFailure,
            picker: Arc::new(Failing { error: err }),
        });
        channel_controller.request_resolution();
        self.sent_connecting_state = false;
    }
}

struct OneSubchannelPicker {
    sc: Arc<dyn Subchannel>,
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

    fn is_first_pass_complete(&self) -> bool {
        if self.current_idx < self.ordered_subchannels.len() {
            return false;
        }
        for data in self.subchannels.values() {
            if !data.seen_transient_failure {
                return false;
            }
        }
        true
    }

    fn connect_to_all_subchannels(&mut self, channel_controller: &mut dyn ChannelController) {
        for (sc, data) in &mut self.subchannels {
            if data.state.as_ref().unwrap().connectivity_state == ConnectivityState::Idle {
                sc.connect();
            }
        }
    }

    fn connect_after_idle(&mut self) {
        for sc in &self.ordered_subchannels {
            sc.connect();
        }
    }
}
