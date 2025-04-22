use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::{watch, Notify};
use tokio::task::AbortHandle;
use tonic::async_trait;

use super::channel::WorkQueueTx;
use super::load_balancing::{self, Picker, Subchannel, SubchannelState, SubchannelUpdate};
use super::name_resolution::Address;
use super::transport::{self, ConnectedTransport, Transport, TransportRegistry};
use super::ConnectivityState;
use crate::client::channel::InternalChannelController;
use crate::service::{Request, Response, Service};

struct TODO;

struct InternalSubchannel {
    t: Arc<dyn Transport>,
    state: Mutex<InternalSubchannelState>,
    address: String,
    backoff: TODO,
    connectivity_state: Box<dyn Fn(SubchannelState) + Send + Sync>,
}

type SharedService = Arc<dyn ConnectedTransport>;

enum InternalSubchannelState {
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

impl InternalSubchannel {
    /// Creates a new subchannel in idle state.
    fn new(
        t: Arc<dyn Transport>,
        address: String,
        connectivity_state: Box<dyn Fn(SubchannelState) + Send + Sync>,
    ) -> Self {
        InternalSubchannel {
            t,
            state: Mutex::new(InternalSubchannelState::Idle),
            address,
            backoff: TODO,
            connectivity_state,
        }
    }

    /// Wait for any in-flight RPCs to terminate and then close the connection
    /// and destroy the Subchannel.
    async fn drain(self) {}

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
                    let svc =
                        s.t.connect(s.address.clone())
                            .await
                            .expect("todo: handle error (go TF w/backoff)");
                    let svc: Arc<dyn ConnectedTransport> = svc.into();
                    s.to_ready(svc.clone());
                    svc.disconnected().await;
                    s.to_idle();
                };
                let jh = tokio::task::spawn(fut);
                *state = InternalSubchannelState::Connecting(jh.abort_handle());
                n.notify_one();
            }
            InternalSubchannelState::TransientFailure(_) => {
                // TODO: remember connect request and skip Idle when expires
            }
            InternalSubchannelState::Ready(_) => {} // Cannot connect while ready.
            InternalSubchannelState::Connecting(_) => {} // Already connecting.
        }
    }

    fn to_ready(&self, svc: Arc<dyn ConnectedTransport>) {
        *self.state.lock().unwrap() = InternalSubchannelState::Ready(svc);
        (self.connectivity_state)(SubchannelState {
            connectivity_state: ConnectivityState::Ready,
            last_connection_error: None,
        });
    }

    fn to_idle(&self) {
        *self.state.lock().unwrap() = InternalSubchannelState::Idle;
        (self.connectivity_state)(SubchannelState {
            connectivity_state: ConnectivityState::Idle,
            last_connection_error: None,
        });
    }

    /*fn connectivity_state(&self) -> ConnectivityState {
        match *self.state.lock().unwrap() {
            InternalSubchannelState::Idle => ConnectivityState::Idle,
            InternalSubchannelState::Connecting(_) => ConnectivityState::Connecting,
            InternalSubchannelState::Ready(_) => ConnectivityState::Ready,
            InternalSubchannelState::TransientFailure(_) => ConnectivityState::TransientFailure,
        }
    }*/
}

impl Drop for InternalSubchannel {
    fn drop(&mut self) {
        // TODO: remove self from pool.
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

pub(crate) struct InternalSubchannelPool {
    transport_registry: TransportRegistry,
    subchannels: Mutex<HashMap<Subchannel, Arc<InternalSubchannel>>>,
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

    pub(crate) async fn call(&self, sc: &Subchannel, request: Request) -> Response {
        let scs = self.subchannels.lock().unwrap();
        let sc = scs.get(sc).expect("Illegal Subchannel in pick");
        return sc.call(request).await;
    }

    pub(super) fn new_subchannel(&self, address: &Address) -> Subchannel {
        println!("creating subchannel for {address}");
        let t = self
            .transport_registry
            .get_transport(&address.address_type)
            .unwrap();

        let n = Arc::new(Notify::new());
        let sc = Subchannel::new(n.clone());
        let sc2 = sc.clone();
        let wtx = self.wtx.clone();
        let cs_update = move |st| {
            //let mut scu = SubchannelUpdate::new();
            //scu.set(&sc2, st);
            let sc = sc2.clone();
            let _ = wtx.send(Box::new(move |c: &mut InternalChannelController| {
                c.lb.clone().subchannel_update(&sc, &st, c);
            }));
        };
        let isc = Arc::new(InternalSubchannel::new(
            t,
            address.address.clone(),
            Box::new(cs_update),
        ));
        self.subchannels
            .lock()
            .unwrap()
            .insert(sc.clone(), isc.clone());

        // TODO: this will never exit or be aborted!
        tokio::task::spawn(async move {
            loop {
                n.notified().await;
                isc.connect(false);
            }
        });

        sc
    }
}
