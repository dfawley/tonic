use std::collections::HashMap;
use std::convert::Into;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Add, Sub};
use std::sync::{
    atomic::{AtomicU32, Ordering::Relaxed},
    Arc, Mutex, Weak,
};
use std::time::Instant;

use tokio::sync::{watch, Notify};
use tokio::task::AbortHandle;
use tonic::async_trait;

use super::channel::WorkQueueTx;
use super::load_balancing::{self, Picker, Subchannel, SubchannelDropNotifier, SubchannelState};
use super::name_resolution::Address;
use super::transport::{self, ConnectedTransport, Transport, TransportRegistry};
use super::ConnectivityState;
use crate::client::channel::InternalChannelController;
use crate::service::{Request, Response, Service};

struct TODO;

type SharedService = Arc<dyn ConnectedTransport>;

pub enum InternalSubchannelState {
    Idle,
    Connecting(AbortHandle),
    Ready(SharedService),
    TransientFailure(Instant),
}

impl InternalSubchannelState {
    fn connected_transport(&self) -> Option<SharedService> {
        match self {
            Self::Ready(t) => Some(t.clone()),
            _ => None,
        }
    }
}

impl Drop for InternalSubchannelState {
    fn drop(&mut self) {
        if let InternalSubchannelState::Connecting(ah) = self {
            ah.abort();
        }
    }
}

pub trait SubchannelImpl: Service + Send + Sync {
    fn connect(&self, now: bool);
    fn subchannel_state(&self) -> SubchannelState;
}

pub(super) struct InternalSubchannel {
    address: Address,
    transport: Arc<dyn Transport>,
    pool: Arc<InternalSubchannelPool>,
    backoff: TODO,
    state: Mutex<InternalSubchannelState>,
}

impl InternalSubchannel {
    /// Creates a new subchannel in idle state.
    pub(crate) fn new(
        address: Address,
        transport: Arc<dyn Transport>,
        pool: Arc<InternalSubchannelPool>,
    ) -> Arc<dyn SubchannelImpl> {
        Arc::new(Self {
            address,
            transport,
            pool,
            backoff: TODO,
            state: Mutex::new(InternalSubchannelState::Idle),
        })
    }

    /// Wait for any in-flight RPCs to terminate and then close the connection
    /// and destroy the Subchannel.
    async fn drain(self) {}

    fn to_ready(&self, svc: Arc<dyn ConnectedTransport>) {
        *self.state.lock().unwrap() = InternalSubchannelState::Ready(svc);
        self.pool.send_connnectivity_state(
            &self.address,
            SubchannelState {
                connectivity_state: ConnectivityState::Ready,
                last_connection_error: None,
            },
        );
    }

    fn to_idle(&self) {
        *self.state.lock().unwrap() = InternalSubchannelState::Idle;
        self.pool.send_connnectivity_state(
            &self.address,
            SubchannelState {
                connectivity_state: ConnectivityState::Idle,
                last_connection_error: None,
            },
        );
    }
}

#[async_trait]
impl Service for InternalSubchannel {
    async fn call(&self, request: Request) -> Response {
        let svc = self
            .state
            .lock()
            .unwrap()
            .connected_transport()
            .expect("todo: handle !ready")
            .clone();
        svc.call(request).await
    }
}

impl SubchannelImpl for InternalSubchannel {
    /// Begins connecting the subchannel asynchronously.  If now is set, does
    /// not wait for any pending connection backoff to complete.
    fn connect(&self, now: bool) {
        let mut state = self.state.lock().unwrap();
        match &*state {
            InternalSubchannelState::Idle => {
                let n = Arc::new(Notify::new());
                let n2 = n.clone();
                // TODO: safe alternative? This task is aborted in drop so self
                // can never outlive it.
                let s = unsafe {
                    std::mem::transmute::<&InternalSubchannel, &'static InternalSubchannel>(self)
                };
                let fut = async move {
                    // Block until the Connecting state is set so we can't race
                    // and set Ready first.
                    n2.notified().await;
                    let svc = s
                        .transport
                        .connect(s.address.address.clone())
                        .await
                        .expect("todo: handle error (go TF w/backoff)");
                    let svc: Arc<dyn ConnectedTransport> = svc.into();
                    s.to_ready(svc.clone());
                    svc.disconnected().await;
                    s.to_idle();
                };
                let jh = tokio::task::spawn(fut);
                *state = InternalSubchannelState::Connecting(jh.abort_handle());
                self.pool.send_connnectivity_state(
                    &self.address,
                    SubchannelState {
                        connectivity_state: ConnectivityState::Connecting,
                        last_connection_error: None,
                    },
                );
                n.notify_one();
            }
            InternalSubchannelState::TransientFailure(_) => {
                // TODO: remember connect request and skip Idle when expires
            }
            InternalSubchannelState::Ready(_) => {} // Cannot connect while ready.
            InternalSubchannelState::Connecting(_) => {} // Already connecting.
        }
    }
    fn subchannel_state(&self) -> SubchannelState {
        match *(self.state.lock().unwrap()) {
            InternalSubchannelState::Idle => SubchannelState {
                connectivity_state: ConnectivityState::Idle,
                last_connection_error: None,
            },
            InternalSubchannelState::Connecting(_) => SubchannelState {
                connectivity_state: ConnectivityState::Connecting,
                last_connection_error: None,
            },
            InternalSubchannelState::Ready(_) => SubchannelState {
                connectivity_state: ConnectivityState::Ready,
                last_connection_error: None,
            },
            // TODO(easwars): Capture the last connection error in the
            // TransientFailure variant and use that to set it here.
            InternalSubchannelState::TransientFailure(t) => SubchannelState {
                connectivity_state: ConnectivityState::TransientFailure,
                last_connection_error: None,
            },
        }
    }
}

type NewSuchannelImplFn = fn(
    Address,
    Arc<dyn Transport>,
    Arc<InternalSubchannelPool>,
) -> Arc<dyn SubchannelImpl + Send + Sync>;

pub(crate) struct InternalSubchannelPool {
    transport_registry: TransportRegistry,
    // TODO(easwars): Ensure that attributes are taken into consideration for
    // equality on the Address type.
    subchannels: Mutex<HashMap<Address, Vec<Weak<Subchannel>>>>,
    wtx: WorkQueueTx,
}

impl InternalSubchannelPool {
    pub(crate) fn new(transport_registry: TransportRegistry, wtx: WorkQueueTx) -> Self {
        Self {
            transport_registry,
            wtx,
            subchannels: Mutex::default(),
        }
    }

    pub fn send_connnectivity_state(&self, address: &Address, state: SubchannelState) {
        let subchannels = self
            .subchannels
            .lock()
            .unwrap()
            .get(&address)
            .unwrap()
            .clone();
        let _ = self
            .wtx
            .send(Box::new(move |c: &mut InternalChannelController| {
                for sc in subchannels.iter() {
                    c.lb.clone()
                        .subchannel_update(sc.upgrade().unwrap(), &state, c);
                }
            }));
    }

    pub(super) fn new_subchannel(
        &self,
        address: Address,
        arc_to_self: Arc<InternalSubchannelPool>,
    ) -> Arc<Subchannel> {
        let mut subchannels = self.subchannels.lock().unwrap();
        if let Some(scs) = subchannels.get_mut(&address) {
            // We expect subchannels in the vector to be promotable, as dropping
            // of subchannels results in pruning of the vector. We also expect
            // map entries to have non-empty vectors.
            let isc: Arc<dyn SubchannelImpl> = scs.first().unwrap().upgrade().unwrap().isc.clone();
            let sc = Arc::new(Subchannel::new(address, arc_to_self.clone(), isc));
            scs.push(Arc::downgrade(&sc));

            let state = sc.isc.subchannel_state();
            let sc2 = Arc::clone(&sc);
            let _ = self
                .wtx
                .send(Box::new(move |c: &mut InternalChannelController| {
                    c.lb.clone().subchannel_update(sc2, &state, c);
                }));
            return sc;
        } else {
            // Create the internal subchannel.
            println!("creating subchannel for {address}");
            let t = self
                .transport_registry
                .get_transport(&address.address_type)
                .unwrap();

            let isc = InternalSubchannel::new(address.clone(), t, arc_to_self.clone());
            let sc = Arc::new(Subchannel::new(address.clone(), arc_to_self.clone(), isc));
            subchannels.insert(address, vec![Arc::downgrade(&sc)]);

            let sc2 = Arc::clone(&sc);
            let _ = self
                .wtx
                .send(Box::new(move |c: &mut InternalChannelController| {
                    c.lb.clone().subchannel_update(
                        sc2,
                        &SubchannelState {
                            connectivity_state: ConnectivityState::Idle,
                            last_connection_error: None,
                        },
                        c,
                    );
                }));
            return sc;
        }
    }
}

impl SubchannelDropNotifier for InternalSubchannelPool {
    fn drop_subchannel(&self, address: &Address) {
        // Remove subchannels whose strong_count is 0.
        let mut subchannels = self.subchannels.lock().unwrap();
        let scs = subchannels.get_mut(address).unwrap();
        scs.retain(|sc| sc.strong_count() > 0);

        // If there are no more subchannels for the given address, remove the
        // map entry as well.
        if scs.len() == 0 {
            subchannels.remove(address);
        }
    }
}
