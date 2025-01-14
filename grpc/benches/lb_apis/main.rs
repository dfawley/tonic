#![allow(unused_imports, dead_code)]

use bencher::{benchmark_group, benchmark_main, Bencher};
use del_pol_single::DelegatingPolicy;
use rand::prelude::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::channel;

use grpc::client::{
    load_balancing::{
        ChannelController, ChannelControllerCallbacks, LbPolicyBatched, LbPolicyCallbacks,
        LbPolicySingle, Subchannel, SubchannelState, SubchannelUpdate, SubchannelUpdateFn,
    },
    name_resolution::{Address, Endpoint, ResolverData, ResolverUpdate},
    ConnectivityState,
};

benchmark_group!(benches, broadcast, single, batched, callbacks);
benchmark_main!(benches);

mod chi_pol_batched;
mod chi_pol_broadcast;
mod chi_pol_cb;
mod chi_pol_single;
mod del_pol_batched;
mod del_pol_broadcast;
mod del_pol_cb;
mod del_pol_single;

static NUM_ENDPOINTS: i32 = 20;
static NUM_ADDRS_PER_ENDPOINT: i32 = 50;
static NUM_SUBCHANNELS_PER_UPDATE: i32 = 1;

fn single(bench: &mut Bencher) {
    let mut lb = DelegatingPolicy::new();

    // Create the ResolverData containing many endpoints and addresses.
    let mut rd = ResolverData::default();
    for i in 0..NUM_ENDPOINTS {
        let mut endpoint = Endpoint::default();
        for j in 0..NUM_ADDRS_PER_ENDPOINT {
            let mut address = Address::default();
            address.address = format!("{i}:{j}");
            address.address_type = "foo".to_string();
            endpoint.addresses.push(address);
        }
        rd.endpoints.push(endpoint);
    }
    let mut channel_controller = StubChannelController::new();
    let _ = lb.resolver_update(ResolverUpdate::Data(rd), None, &mut channel_controller);
    let num_subchannels = channel_controller.subchannels.len();

    bench.iter(|| {
        // Update random subchannels to a random state.
        for _ in 0..NUM_SUBCHANNELS_PER_UPDATE {
            let connectivity_state = thread_rng().gen_range(0..4);
            let connectivity_state = match connectivity_state {
                0 => ConnectivityState::Idle,
                1 => ConnectivityState::Connecting,
                2 => ConnectivityState::Ready,
                _ => ConnectivityState::TransientFailure,
            };
            let sc =
                channel_controller.subchannels[thread_rng().gen_range(0..num_subchannels)].clone();
            lb.subchannel_update(
                &sc,
                &SubchannelState {
                    connectivity_state,
                    last_connection_error: None,
                },
                &mut channel_controller,
            );
        }
    });
}

fn batched(bench: &mut Bencher) {
    let mut lb = del_pol_batched::DelegatingPolicy::new();

    // Create the ResolverData containing many endpoints and addresses.
    let mut rd = ResolverData::default();
    for i in 0..NUM_ENDPOINTS {
        let mut endpoint = Endpoint::default();
        for j in 0..NUM_ADDRS_PER_ENDPOINT {
            let mut address = Address::default();
            address.address = format!("{i}:{j}");
            address.address_type = "foo".to_string();
            endpoint.addresses.push(address);
        }
        rd.endpoints.push(endpoint);
    }
    let mut channel_controller = StubChannelController::new();
    let _ = lb.resolver_update(ResolverUpdate::Data(rd), None, &mut channel_controller);
    let num_subchannels = channel_controller.subchannels.len();

    bench.iter(|| {
        let mut update = SubchannelUpdate::new();
        // Update random subchannels to a random state.
        for _ in 0..NUM_SUBCHANNELS_PER_UPDATE {
            let connectivity_state = thread_rng().gen_range(0..4);
            let connectivity_state = match connectivity_state {
                0 => ConnectivityState::Idle,
                1 => ConnectivityState::Connecting,
                2 => ConnectivityState::Ready,
                _ => ConnectivityState::TransientFailure,
            };
            let sc =
                channel_controller.subchannels[thread_rng().gen_range(0..num_subchannels)].clone();
            update.set(
                &sc,
                SubchannelState {
                    connectivity_state,
                    last_connection_error: None,
                },
            );
        }
        lb.subchannel_update(&update, &mut channel_controller);
    });
}

fn broadcast(bench: &mut Bencher) {
    let mut lb = del_pol_broadcast::DelegatingPolicy::new();

    // Create the ResolverData containing many endpoints and addresses.
    let mut rd = ResolverData::default();
    for i in 0..NUM_ENDPOINTS {
        let mut endpoint = Endpoint::default();
        for j in 0..NUM_ADDRS_PER_ENDPOINT {
            let mut address = Address::default();
            address.address = format!("{i}:{j}");
            address.address_type = "foo".to_string();
            endpoint.addresses.push(address);
        }
        rd.endpoints.push(endpoint);
    }
    let mut channel_controller = StubChannelController::new();
    let _ = lb.resolver_update(ResolverUpdate::Data(rd), None, &mut channel_controller);
    let num_subchannels = channel_controller.subchannels.len();

    bench.iter(|| {
        let mut update = SubchannelUpdate::new();
        // Update random subchannels to a random state.
        for _ in 0..NUM_SUBCHANNELS_PER_UPDATE {
            let connectivity_state = thread_rng().gen_range(0..4);
            let connectivity_state = match connectivity_state {
                0 => ConnectivityState::Idle,
                1 => ConnectivityState::Connecting,
                2 => ConnectivityState::Ready,
                _ => ConnectivityState::TransientFailure,
            };
            let sc =
                channel_controller.subchannels[thread_rng().gen_range(0..num_subchannels)].clone();
            update.set(
                &sc,
                SubchannelState {
                    connectivity_state,
                    last_connection_error: None,
                },
            );
        }
        lb.subchannel_update(&update, &mut channel_controller);
    });
}

pub struct StubChannelController {
    pub subchannels: Vec<Subchannel>,
}

impl StubChannelController {
    pub fn new() -> Self {
        Self {
            subchannels: vec![],
        }
    }
}

impl ChannelController for StubChannelController {
    fn new_subchannel(&mut self, _: &grpc::client::name_resolution::Address) -> Subchannel {
        // Just return a new, empty subchannel, ignoring the address and connect
        // notifications.
        let sc = Subchannel::new(Arc::default());
        self.subchannels.push(sc.clone());
        sc
    }

    fn update_picker(&mut self, _: grpc::client::load_balancing::LbState) {
        // Do nothing with the update.
    }

    fn request_resolution(&mut self) {
        // No resolver to notify.
    }
}

fn callbacks(bench: &mut Bencher) {
    let channel_controller = Arc::new(StubChannelControllerCallbacks::new());
    let mut lb = del_pol_cb::DelegatingPolicy::new(channel_controller.clone());

    // Create the ResolverData containing many endpoints and addresses.
    let mut rd = ResolverData::default();
    for i in 0..NUM_ENDPOINTS {
        let mut endpoint = Endpoint::default();
        for j in 0..NUM_ADDRS_PER_ENDPOINT {
            let mut address = Address::default();
            address.address = format!("{i}:{j}");
            address.address_type = "foo".to_string();
            endpoint.addresses.push(address);
        }
        rd.endpoints.push(endpoint);
    }
    let _ = lb.resolver_update(ResolverUpdate::Data(rd), None);
    let num_subchannels = channel_controller.subchannels.lock().unwrap().len();

    bench.iter(|| {
        // Update random subchannels to a random state.
        for _ in 0..NUM_SUBCHANNELS_PER_UPDATE {
            let connectivity_state = thread_rng().gen_range(0..4);
            let connectivity_state = match connectivity_state {
                0 => ConnectivityState::Idle,
                1 => ConnectivityState::Connecting,
                2 => ConnectivityState::Ready,
                _ => ConnectivityState::TransientFailure,
            };
            channel_controller.send_update(
                thread_rng().gen_range(0..num_subchannels),
                connectivity_state,
            );
        }
    });
}

pub struct StubChannelControllerCallbacks {
    pub subchannels: Mutex<Vec<Arc<(Subchannel, SubchannelUpdateFn)>>>,
}

impl StubChannelControllerCallbacks {
    pub fn new() -> Self {
        Self {
            subchannels: Mutex::default(),
        }
    }
}

impl StubChannelControllerCallbacks {
    fn send_update(&self, n: usize, connectivity_state: ConnectivityState) {
        let x = self.subchannels.lock().unwrap()[n].clone();
        x.1(
            x.0.clone(),
            SubchannelState {
                connectivity_state,
                last_connection_error: None,
            },
        );
    }
}

impl ChannelControllerCallbacks for StubChannelControllerCallbacks {
    fn new_subchannel(&self, _: &Address, updates: SubchannelUpdateFn) -> Subchannel {
        // Just return a new, empty subchannel, ignoring the address and connect
        // notifications.
        let sc = Subchannel::new(Arc::default());
        self.subchannels
            .lock()
            .unwrap()
            .push(Arc::new((sc.clone(), updates)));
        sc
    }

    fn update_picker(&self, _: grpc::client::load_balancing::LbState) {
        // Do nothing with the update.
    }

    fn request_resolution(&self) {
        // No resolver to notify.
    }
}

pub(crate) fn effective_state(m: impl Iterator<Item = ConnectivityState>) -> ConnectivityState {
    let mut connectivity_state = ConnectivityState::TransientFailure;

    for con_state in m {
        if con_state == ConnectivityState::Ready {
            connectivity_state = ConnectivityState::Ready;
        } else if con_state == ConnectivityState::Connecting
            && connectivity_state != ConnectivityState::Ready
        {
            connectivity_state = ConnectivityState::Connecting;
        } else if con_state == ConnectivityState::Idle
            && connectivity_state != ConnectivityState::Connecting
            && connectivity_state != ConnectivityState::Ready
        {
            connectivity_state = ConnectivityState::Idle;
        } else if connectivity_state != ConnectivityState::Ready
            && connectivity_state != ConnectivityState::Connecting
            && connectivity_state != ConnectivityState::Idle
        {
            connectivity_state = ConnectivityState::TransientFailure;
        }
    }
    connectivity_state
}
