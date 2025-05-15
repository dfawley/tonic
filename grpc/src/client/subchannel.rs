use super::{
    channel::{InternalChannelController, WorkQueueTx},
    load_balancing::{self, Picker, Subchannel, SubchannelState},
    name_resolution::Address,
    transport::{self, ConnectedTransport, Transport, TransportRegistry},
    ConnectivityState,
};
use crate::service::{Request, Response, Service};
use core::panic;
use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{Debug, Display},
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::{mpsc, watch, Notify},
    task::{AbortHandle, JoinHandle},
    time::{Duration, Instant},
};
use tonic::async_trait;

type SharedService = Arc<dyn ConnectedTransport>;

pub trait Backoff: Send + Sync {
    fn backoff_until(&self) -> Instant;
    fn reset(&self);
    fn min_connect_timeout(&self) -> Duration;
}

// TODO(easwars): Move this somewhere else, where appropriate.
pub(crate) struct NopBackoff {}
impl Backoff for NopBackoff {
    fn backoff_until(&self) -> Instant {
        Instant::now()
    }
    fn reset(&self) {}
    fn min_connect_timeout(&self) -> Duration {
        Duration::from_secs(20)
    }
}

enum InternalSubchannelState {
    Idle,
    Connecting(InternalSubchannelConnectingState),
    Ready(InternalSubchannelReadyState),
    TransientFailure(InternalSubchannelTransientFailureState),
}

struct InternalSubchannelConnectingState {
    abort_handle: Option<AbortHandle>,
}

struct InternalSubchannelReadyState {
    abort_handle: Option<AbortHandle>,
    svc: SharedService,
}

struct InternalSubchannelTransientFailureState {
    abort_handle: Option<AbortHandle>,
    error: String,
}

impl InternalSubchannelState {
    fn connected_transport(&self) -> Option<SharedService> {
        match self {
            Self::Ready(st) => Some(st.svc.clone()),
            _ => None,
        }
    }

    fn to_subchannel_state(&self) -> SubchannelState {
        match self {
            Self::Idle => SubchannelState {
                connectivity_state: ConnectivityState::Idle,
                last_connection_error: None,
            },
            Self::Connecting(_) => SubchannelState {
                connectivity_state: ConnectivityState::Connecting,
                last_connection_error: None,
            },
            Self::Ready(_) => SubchannelState {
                connectivity_state: ConnectivityState::Ready,
                last_connection_error: None,
            },
            Self::TransientFailure(st) => {
                let arc_err: Arc<dyn Error + Send + Sync> = Arc::from(Box::from(st.error.clone()));
                SubchannelState {
                    connectivity_state: ConnectivityState::TransientFailure,
                    last_connection_error: Some(arc_err),
                }
            }
        }
    }
}

impl Display for InternalSubchannelState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::Connecting(_) => write!(f, "Connecting"),
            Self::Ready(_) => write!(f, "Ready"),
            Self::TransientFailure(_) => write!(f, "TransientFailure"),
        }
    }
}

impl Debug for InternalSubchannelState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::Connecting(_) => write!(f, "Connecting"),
            Self::Ready(_) => write!(f, "Ready"),
            Self::TransientFailure(_) => write!(f, "TransientFailure"),
        }
    }
}

impl PartialEq for InternalSubchannelState {
    fn eq(&self, other: &Self) -> bool {
        match &self {
            Self::Idle => {
                if let Self::Idle = other {
                    return true;
                }
            }
            Self::Connecting(_) => {
                if let Self::Connecting(_) = other {
                    return true;
                }
            }
            Self::Ready(_) => {
                if let Self::Ready(_) = other {
                    return true;
                }
            }
            Self::TransientFailure(_) => {
                if let Self::TransientFailure(_) = other {
                    return true;
                }
            }
        }
        false
    }
}

impl Drop for InternalSubchannelState {
    fn drop(&mut self) {
        match &self {
            Self::Idle => {}
            Self::Connecting(st) => {
                if let Some(ah) = &st.abort_handle {
                    ah.abort();
                }
            }
            Self::Ready(st) => {
                if let Some(ah) = &st.abort_handle {
                    ah.abort();
                }
            }
            Self::TransientFailure(st) => {
                if let Some(ah) = &st.abort_handle {
                    ah.abort();
                }
            }
        }
    }
}

pub(crate) struct InternalSubchannel {
    key: SubchannelKey,
    work_scheduler: WorkQueueTx,
    transport: Arc<dyn Transport>,
    backoff: Arc<dyn Backoff>,
    state_machine_event_sender: mpsc::UnboundedSender<SubchannelStateMachineEvent>,
    inner: Arc<Mutex<InnerSubchannel>>,
}

struct InnerSubchannel {
    state: InternalSubchannelState,
    watchers: Vec<Arc<SubchannelStateWatcher>>, // TODO(easwars): Revisit the choice for this data structure.
    backoff_task: Option<JoinHandle<()>>,
    disconnect_task: Option<JoinHandle<()>>,
}

#[async_trait]
impl Service for InternalSubchannel {
    async fn call(&self, method: String, request: Request) -> Response {
        let svc = self.inner.lock().unwrap().state.connected_transport();
        if svc.is_none() {
            // TODO(easwars): Change the signature of this method to return a
            // Result<Response, Error>
            panic!("todo: handle !ready");
        }

        let svc = svc.unwrap().clone();
        return svc.clone().call(method, request).await;
    }
}

enum SubchannelStateMachineEvent {
    ConnectionRequested,
    ConnectionSucceeded(SharedService),
    ConnectionTimedOut,
    ConnectionFailed(String),
    ConnectionTerminated,
    BackoffExpired,
}
impl Debug for SubchannelStateMachineEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionRequested => write!(f, "ConnectionRequested"),
            Self::ConnectionSucceeded(_) => write!(f, "ConnectionSucceeded"),
            Self::ConnectionTimedOut => write!(f, "ConnectionTimedOut"),
            Self::ConnectionFailed(_) => write!(f, "ConnectionFailed"),
            Self::ConnectionTerminated => write!(f, "ConnectionTerminated"),
            Self::BackoffExpired => write!(f, "BackoffExpired"),
        }
    }
}

impl InternalSubchannel {
    pub(super) fn new(
        key: SubchannelKey,
        work_scheduler: WorkQueueTx,
        transport: Arc<dyn Transport>,
        backoff: Arc<dyn Backoff>,
    ) -> Arc<InternalSubchannel> {
        println!("creating new internal subchannel for: {:?}", &key);
        let (tx, mut rx) = mpsc::unbounded_channel::<SubchannelStateMachineEvent>();
        let isc = Arc::new(Self {
            key: key.clone(),
            work_scheduler,
            transport,
            backoff: backoff.clone(),
            state_machine_event_sender: tx,
            inner: Arc::new(Mutex::new(InnerSubchannel {
                state: InternalSubchannelState::Idle,
                watchers: Vec::new(),
                backoff_task: None,
                disconnect_task: None,
            })),
        });

        // This long running task implements the subchannel state machine. When
        // the subchannel is dropped, the channel from which this task reads is
        // closed, and therefore this task exits because rx.recv() returns None
        // in that case.
        let arc_to_self = Arc::clone(&isc);
        tokio::task::spawn(async move {
            println!("starting subchannel state machine for: {:?}", &key);
            while let Some(m) = rx.recv().await {
                println!("subchannel {:?} received event {:?}", &key, &m);
                match m {
                    SubchannelStateMachineEvent::ConnectionRequested => {
                        arc_to_self.move_to_connecting();
                    }
                    SubchannelStateMachineEvent::ConnectionSucceeded(svc) => {
                        arc_to_self.move_to_ready(svc);
                    }
                    SubchannelStateMachineEvent::ConnectionTimedOut => {
                        arc_to_self.move_to_transient_failure("connect timeout expired".into());
                    }
                    SubchannelStateMachineEvent::ConnectionFailed(err) => {
                        arc_to_self.move_to_transient_failure(err);
                    }
                    SubchannelStateMachineEvent::ConnectionTerminated => {
                        arc_to_self.move_to_idle();
                    }
                    SubchannelStateMachineEvent::BackoffExpired => {
                        arc_to_self.move_to_idle();
                    }
                }
            }
            println!("exiting work queue task in subchannel");
        });
        isc
    }

    pub(crate) fn address(&self) -> &Address {
        &self.key.address
    }

    /// Begins connecting the subchannel asynchronously.  If now is set, does
    /// not wait for any pending connection backoff to complete.
    pub(crate) fn connect(&self, now: bool) {
        let state = &self.inner.lock().unwrap().state;
        if let InternalSubchannelState::Idle = state {
            let _ = self
                .state_machine_event_sender
                .send(SubchannelStateMachineEvent::ConnectionRequested);
        }
    }

    pub(crate) fn register_connectivity_state_watcher(&self, watcher: Arc<SubchannelStateWatcher>) {
        let mut inner = self.inner.lock().unwrap();
        inner.watchers.push(watcher.clone());
        let state = inner.state.to_subchannel_state().clone();
        let _ = self
            .work_scheduler
            .send(Box::new(move |c: &mut InternalChannelController| {
                watcher.on_state_change(state, c);
            }));
    }

    pub(crate) fn unregister_connectivity_state_watcher(
        &self,
        watcher: Arc<SubchannelStateWatcher>,
    ) {
        self.inner
            .lock()
            .unwrap()
            .watchers
            .retain(|x| !Arc::ptr_eq(x, &watcher));
    }

    fn notify_watchers(&self, state: SubchannelState) {
        // TODO(easwars): See if there is a way to call all watchers in a single
        // work serializer task. Currently, that is not possible since we need
        // to capture a reference to self in the closure, and self may not live
        // long enough.
        let mut inner = self.inner.lock().unwrap();
        inner.state = InternalSubchannelState::Idle;
        for w in &inner.watchers {
            let state = state.clone();
            let w = w.clone();
            let _ = self
                .work_scheduler
                .send(Box::new(move |c: &mut InternalChannelController| {
                    w.on_state_change(state, c);
                }));
        }
    }

    fn move_to_idle(&self) {
        self.notify_watchers(SubchannelState {
            connectivity_state: ConnectivityState::Idle,
            last_connection_error: None,
        });
    }

    fn move_to_connecting(&self) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.state = InternalSubchannelState::Connecting(InternalSubchannelConnectingState {
                abort_handle: None,
            });
        }
        self.notify_watchers(SubchannelState {
            connectivity_state: ConnectivityState::Connecting,
            last_connection_error: None,
        });

        let min_connect_timeout = self.backoff.min_connect_timeout();
        let transport = self.transport.clone();
        let address = self.address().address.clone();
        let state_machine_tx = self.state_machine_event_sender.clone();
        let connect_task = tokio::task::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(min_connect_timeout) => {
                    let _ = state_machine_tx.send(SubchannelStateMachineEvent::ConnectionTimedOut);
                }
                result = transport.connect(address.clone()) => {
                    match result {
                        Ok(s) => {
                            let _ = state_machine_tx.send(SubchannelStateMachineEvent::ConnectionSucceeded(Arc::from(s)));
                        }
                        Err(e) => {
                            let _ = state_machine_tx.send(SubchannelStateMachineEvent::ConnectionFailed(e));
                        }
                    }
                },
            }
        });
        let mut inner = self.inner.lock().unwrap();
        inner.state = InternalSubchannelState::Connecting(InternalSubchannelConnectingState {
            abort_handle: Some(connect_task.abort_handle()),
        });
    }

    fn move_to_ready(&self, svc: SharedService) {
        let svc2 = svc.clone();
        {
            let mut inner = self.inner.lock().unwrap();
            inner.state = InternalSubchannelState::Ready(InternalSubchannelReadyState {
                abort_handle: None,
                svc: svc2.clone(),
            });
        }
        self.notify_watchers(SubchannelState {
            connectivity_state: ConnectivityState::Ready,
            last_connection_error: None,
        });

        let state_machine_tx = self.state_machine_event_sender.clone();
        let disconnect_task = tokio::task::spawn(async move {
            // TODO(easwars): Does it make sense for disconnected() to return an
            // error string containing information about why the connection
            // terminated? But what can we do with that error other than logging
            // it, which the transport can do as well?
            svc.disconnected().await;
            let _ = state_machine_tx.send(SubchannelStateMachineEvent::ConnectionTerminated);
        });
        let mut inner = self.inner.lock().unwrap();
        inner.state = InternalSubchannelState::Ready(InternalSubchannelReadyState {
            abort_handle: Some(disconnect_task.abort_handle()),
            svc: svc2.clone(),
        });
    }

    fn move_to_transient_failure(&self, err: String) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.state = InternalSubchannelState::TransientFailure(
                InternalSubchannelTransientFailureState {
                    abort_handle: None,
                    error: err.clone(),
                },
            );
        }

        let arc_err: Arc<dyn Error + Send + Sync> = Arc::from(Box::from(err.clone()));
        self.notify_watchers(SubchannelState {
            connectivity_state: ConnectivityState::TransientFailure,
            last_connection_error: Some(arc_err.clone()),
        });

        let backoff_interval = self.backoff.backoff_until();
        let state_machine_tx = self.state_machine_event_sender.clone();
        let backoff_task = tokio::task::spawn(async move {
            tokio::time::sleep_until(backoff_interval).await;
            let _ = state_machine_tx.send(SubchannelStateMachineEvent::BackoffExpired);
        });
        let mut inner = self.inner.lock().unwrap();
        inner.state =
            InternalSubchannelState::TransientFailure(InternalSubchannelTransientFailureState {
                abort_handle: Some(backoff_task.abort_handle()),
                error: err.clone(),
            });
    }

    /// Wait for any in-flight RPCs to terminate and then close the connection
    /// and destroy the Subchannel.
    async fn drain(self) {}
}

impl Drop for InternalSubchannel {
    fn drop(&mut self) {
        println!("dropping internal subchannel {:?}", self.key);
        let key = self.key.clone();
        let _ = self
            .work_scheduler
            .send(Box::new(move |c: &mut InternalChannelController| {
                c.subchannel_pool.unregister_subchannel(&key);
            }));
    }
}

// SubchannelKey uniiquely identifies a subchannel in the pool.
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]

pub(crate) struct SubchannelKey {
    address: Address,
}

impl SubchannelKey {
    pub(crate) fn new(address: Address) -> Self {
        Self { address }
    }
}

pub(crate) struct InternalSubchannelPool {
    work_scheduler: WorkQueueTx,
    transport_registry: TransportRegistry,
    subchannels: BTreeMap<SubchannelKey, Weak<InternalSubchannel>>,
}

impl InternalSubchannelPool {
    pub(crate) fn new(work_scheduler: WorkQueueTx, transport_registry: TransportRegistry) -> Self {
        Self {
            work_scheduler,
            transport_registry,
            subchannels: BTreeMap::new(),
        }
    }

    pub(crate) fn get_or_create_subchannel(
        &mut self,
        key: &SubchannelKey,
    ) -> Arc<InternalSubchannel> {
        println!("looking up subchannel for: {:?}", key);
        if let Some(weak_isc) = self.subchannels.get(key) {
            if let Some(isc) = weak_isc.upgrade() {
                return isc;
            }
        }

        // If we get here, it means one of two things:
        // 1. provided key is not found in the map
        // 2. provided key points to an unpromotable value, which can occur if
        //    its internal subchannel has been dropped but hasn't been
        //    unregistered yet.

        let transport = self
            .transport_registry
            .get_transport(&key.address.address_type)
            .unwrap();
        let isc = InternalSubchannel::new(
            key.clone(),
            self.work_scheduler.clone(),
            transport,
            Arc::new(NopBackoff {}),
        );
        self.subchannels.insert(key.clone(), Arc::downgrade(&isc));
        isc
    }

    pub(crate) fn unregister_subchannel(&mut self, key: &SubchannelKey) {
        if let Some(weak_isc) = self.subchannels.get(key) {
            if let Some(isc) = weak_isc.upgrade() {
                return;
            }
            self.subchannels.remove(key);
            return;
        }
        panic!("attempt to unregister subchannel for unknown key {:?}", key);
    }
}

#[derive(Clone)]
pub(crate) struct SubchannelStateWatcher {
    subchannel: Weak<dyn Subchannel>,
}

impl SubchannelStateWatcher {
    pub(crate) fn new(sc: Arc<dyn Subchannel>) -> Self {
        Self {
            subchannel: Arc::downgrade(&sc),
        }
    }

    fn on_state_change(&self, state: SubchannelState, c: &mut InternalChannelController) {
        // Ignore internal subchannel state changes if the external subchannel
        // was dropped but its state watcher is still pending unregistration;
        // such updates are inconsequential.
        if let Some(sc) = self.subchannel.upgrade() {
            c.lb.clone()
                .policy
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .subchannel_update(sc, &state, c);
        }
    }
}
