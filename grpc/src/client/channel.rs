use core::panic;
use std::{
    error::Error,
    fmt::Display,
    mem,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
    vec,
};

use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio::task::AbortHandle;

use serde_json::json;
use tonic::async_trait;
use url::Url; // NOTE: http::Uri requires non-empty authority portion of URI

use crate::attributes::Attributes;
use crate::client::service_config::ServiceConfig;
use crate::client::ConnectivityState;
use crate::credentials::Credentials;
use crate::rt;
use crate::service::{Request, Response, Service};

use super::load_balancing::{
    self, pick_first, LbPolicyBuilderSingle, LbPolicyOptions, LbPolicyRegistry, LbPolicySingle,
    LbState, ParsedJsonLbConfig, PickResult, Picker, Subchannel, SubchannelState, WorkScheduler,
    GLOBAL_LB_REGISTRY,
};
use super::name_resolution::{
    self, Address, ResolverBuilder, ResolverOptions, ResolverRegistry, ResolverUpdate,
    GLOBAL_RESOLVER_REGISTRY,
};
use super::subchannel::{InternalSubchannel, InternalSubchannelPool};
use super::transport::{TransportRegistry, GLOBAL_TRANSPORT_REGISTRY};

#[non_exhaustive]
pub struct ChannelOptions {
    pub transport_options: Attributes, // ?
    pub override_authority: Option<String>,
    pub connection_backoff: Option<TODO>,
    pub default_service_config: Option<String>,
    pub disable_proxy: bool,
    pub disable_service_config_lookup: bool,
    pub disable_health_checks: bool,
    pub max_retry_memory: u32, // ?
    pub idle_timeout: Duration,
    pub transport_registry: Option<TransportRegistry>,
    pub name_resolver_registry: Option<LbPolicyRegistry>,
    pub lb_policy_registry: Option<ResolverRegistry>,

    // Typically we allow settings at the channel level that impact all RPCs,
    // but can also be set per-RPC.  E.g.s:
    //
    // - interceptors
    // - user-agent string override
    // - max message sizes
    // - max retry/hedged attempts
    // - disable retry
    //
    // In gRPC-Go, we can express CallOptions as DialOptions, which is a nice
    // pattern: https://pkg.go.dev/google.golang.org/grpc#WithDefaultCallOptions
    //
    // To do this in rust, all optional behavior for a request would need to be
    // expressed through a trait that applies a mutation to a request.  We'd
    // apply all those mutations before the user's options so the user's options
    // would override the defaults, or so the defaults would occur first.
    pub default_request_extensions: Vec<Box<TODO>>, // ??
}

impl Default for ChannelOptions {
    fn default() -> Self {
        Self {
            transport_options: Attributes::new(),
            override_authority: None,
            connection_backoff: None,
            default_service_config: None,
            disable_proxy: false,
            disable_service_config_lookup: false,
            disable_health_checks: false,
            max_retry_memory: 8 * 1024 * 1024, // 8MB -- ???
            idle_timeout: Duration::from_secs(30 * 60),
            name_resolver_registry: None,
            lb_policy_registry: None,
            default_request_extensions: vec![],
            transport_registry: None,
        }
    }
}

impl ChannelOptions {
    pub fn transport_options(self, transport_options: TODO) -> Self {
        todo!(); // add to existing options.
    }
    pub fn override_authority(self, authority: String) -> Self {
        Self {
            override_authority: Some(authority),
            ..self
        }
    }
    pub fn transport_registry(self, registry: TransportRegistry) -> Self {
        Self {
            transport_registry: Some(registry),
            ..self
        }
    }
    // etc
}

// All of Channel needs to be thread-safe.  Arc<inner>?  Or give out
// Arc<Channel> from constructor?
#[derive(Clone)]
pub struct Channel {
    inner: Arc<PersistentChannel>,
}

impl Channel {
    pub fn new(
        target: &str,
        credentials: Option<Box<dyn Credentials>>,
        runtime: Option<Box<dyn rt::Runtime>>,
        options: ChannelOptions,
    ) -> Self {
        Self {
            inner: Arc::new(PersistentChannel::new(
                target,
                credentials,
                runtime,
                options,
            )),
        }
    }
    // Waits until all outstanding RPCs are completed, then stops the client
    // (via "drop"? no, that makes no sense).  Note that there probably needs to
    // be a way to add a timeout here or for the application to do a hard
    // failure while waiting.  Or maybe this feature isn't necessary - Go
    // doesn't have it.  Users can determine on their own if they have
    // outstanding calls.  Some users have requested this feature for Go,
    // nonetheless.
    pub async fn graceful_stop(self) {}

    // Causes the channel to enter idle mode immediately.  Any pending RPCs
    // will continue on existing connections.
    // TODO: do we want this?  Go does not have this.
    //pub async fn enter_idle(&self) {}

    /// Returns the current state of the channel.
    pub fn state(&mut self, connect: bool) -> ConnectivityState {
        let ac = if !connect {
            // If !connect and we have no active channel already, return idle.
            let ac = self.inner.active_channel.lock().unwrap();
            if ac.is_none() {
                return ConnectivityState::Idle;
            }
            ac.as_ref().unwrap().clone()
        } else {
            // Otherwise, get or create the active channel.
            self.get_or_create_active_channel()
        };
        if let Some(s) = ac.connectivity_state.cur() {
            return s;
        }
        ConnectivityState::Idle
    }

    /// Waits for the state of the channel to change from source.  Times out and
    /// returns an error after the deadline.
    // TODO: do we want this or another API based on streaming?  Probably
    // something like the Watcher<T> would be nice.
    pub async fn wait_for_state_change(
        &self,
        source: ConnectivityState,
        deadline: Instant,
    ) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn get_or_create_active_channel(&self) -> Arc<ActiveChannel> {
        let mut s = self.inner.active_channel.lock().unwrap();
        if s.is_none() {
            *s = Some(ActiveChannel::new(
                self.inner.target.clone(),
                &self.inner.options,
            ));
        }
        s.clone().unwrap()
    }

    pub async fn call(&self, request: Request) -> Response {
        let ac = self.get_or_create_active_channel();
        ac.call(request).await
    }
}

struct PersistentChannel {
    target: Url,
    options: ChannelOptions,
    active_channel: Mutex<Option<Arc<ActiveChannel>>>,
}

impl PersistentChannel {
    // Channels begin idle so new is a simple constructor.  Required parameters
    // are not in ChannelOptions.
    fn new(
        target: &str,
        credentials: Option<Box<dyn Credentials>>,
        runtime: Option<Box<dyn rt::Runtime>>,
        options: ChannelOptions,
    ) -> Self {
        Self {
            target: Url::from_str(target).unwrap(), // TODO handle err
            active_channel: Mutex::default(),
            options,
        }
    }
}

struct ActiveChannel {
    cur_state: Mutex<ConnectivityState>,
    abort_handle: AbortHandle,
    picker: Arc<Watcher<Arc<dyn Picker>>>,
    connectivity_state: Arc<Watcher<ConnectivityState>>,
}

impl ActiveChannel {
    fn new(target: Url, options: &ChannelOptions) -> Arc<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel::<WorkQueueItem>();
        let tr = match &options.transport_registry {
            Some(tr) => tr.clone(),
            None => GLOBAL_TRANSPORT_REGISTRY.clone(),
        };
        let scp = Arc::new(InternalSubchannelPool::new(tr, tx.clone()));

        let resolve_now = Arc::new(Notify::new());

        let connectivity_state = Arc::new(Watcher::new());
        let picker = Arc::new(Watcher::new());
        let mut channel_controller = InternalChannelController::new(
            target.clone(),
            scp,
            resolve_now.clone(),
            tx.clone(),
            picker.clone(),
            connectivity_state.clone(),
        );

        let jh = tokio::task::spawn(async move {
            while let Some(w) = rx.recv().await {
                w(&mut channel_controller);
            }
        });

        tokio::task::spawn(async move {
            let resolver_helper = Box::new(tx.clone());
            let rb = GLOBAL_RESOLVER_REGISTRY.get_scheme(target.scheme());
            let mut resolver = rb
                .unwrap()
                .build(target, resolve_now, ResolverOptions::default());

            resolver.run(resolver_helper).await;
        });

        Arc::new(Self {
            cur_state: Mutex::new(ConnectivityState::Connecting),
            abort_handle: jh.abort_handle(),
            picker: picker.clone(),
            connectivity_state: connectivity_state.clone(),
        })
    }

    async fn call(&self, request: Request) -> Response {
        // pre-pick tasks (e.g. deadlines, interceptors, retry)
        // start attempt
        // pick subchannel
        // perform attempt on transport
        let mut i = self.picker.iter();
        loop {
            if let Some(p) = i.next().await {
                let result = &p.pick(&request);
                // TODO: handle picker errors (queue or fail RPC)
                match result {
                    PickResult::Pick(pr) => {
                        return pr.subchannel.isc.call(request).await;
                    }
                    PickResult::Queue => {
                        // Continue and retry the RPC with the next picker.
                    }
                    PickResult::Fail(status) => {
                        panic!("failed pick: {}", status);
                    }
                    PickResult::Drop(status) => {
                        panic!("dropped pick: {}", status);
                    }
                }
            }
        }
    }
}

impl Drop for ActiveChannel {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

pub(super) type WorkQueueTx = mpsc::UnboundedSender<WorkQueueItem>;

#[async_trait]
impl name_resolution::ChannelController for WorkQueueTx {
    fn parse_config(&self, config: &str) -> Result<ServiceConfig, Box<dyn Error + Send + Sync>> {
        Ok(ServiceConfig {})
        // Needs to call gsb's policy builder
    }
    async fn update(&self, state: ResolverUpdate) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (tx, rx) = oneshot::channel();
        self.send(Box::new(|c: &mut InternalChannelController| {
            let _ = tx.send(c.lb.clone().handle_resolver_update(state, c));
        }))?;
        rx.await?
    }
}

pub(super) struct InternalChannelController {
    pub(super) lb: Arc<GracefulSwitchBalancer>, // called and passes mutable parent to it, so must be Arc.
    scp: Arc<InternalSubchannelPool>,
    resolve_now: Arc<Notify>,
    picker: Arc<Watcher<Arc<dyn Picker>>>,
    connectivity_state: Arc<Watcher<ConnectivityState>>,
}

impl InternalChannelController {
    fn new(
        target: Url,
        scp: Arc<InternalSubchannelPool>,
        resolve_now: Arc<Notify>,
        wqtx: WorkQueueTx,
        picker: Arc<Watcher<Arc<dyn Picker>>>,
        connectivity_state: Arc<Watcher<ConnectivityState>>,
    ) -> Self {
        let lb = Arc::new(GracefulSwitchBalancer::new(wqtx));

        Self {
            lb,
            scp,
            resolve_now,
            picker,
            connectivity_state,
        }
    }
}

impl load_balancing::ChannelController for InternalChannelController {
    fn new_subchannel(&mut self, address: &Address) -> Arc<Subchannel> {
        self.scp.new_subchannel(address.clone(), self.scp.clone())
    }

    fn update_picker(&mut self, update: LbState) {
        println!(
            "update picker called with state: {:?}",
            update.connectivity_state
        );
        self.picker.update(update.picker);
        self.connectivity_state.update(update.connectivity_state);
    }

    fn request_resolution(&mut self) {
        self.resolve_now.notify_one();
    }
}

// A channel that is not idle (connecting, ready, or erroring).
pub(super) struct GracefulSwitchBalancer {
    policy: Mutex<Option<Box<dyn LbPolicySingle>>>,
    policy_builder: Mutex<Option<Arc<dyn LbPolicyBuilderSingle>>>,
    work_scheduler: WorkQueueTx,
    pending: Mutex<bool>,
}

impl WorkScheduler for GracefulSwitchBalancer {
    fn schedule_work(&self) {
        if mem::replace(&mut *self.pending.lock().unwrap(), true) {
            // Already had a pending call scheduled.
            return;
        }
        let _ = self
            .work_scheduler
            .send(Box::new(|c: &mut InternalChannelController| {
                *c.lb.pending.lock().unwrap() = false;
                c.lb.clone()
                    .policy
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .work(c);
            }));
    }
}

impl GracefulSwitchBalancer {
    fn new(work_scheduler: WorkQueueTx) -> Self {
        Self {
            policy_builder: Mutex::default(),
            policy: Mutex::default(), // new(None::<Box<dyn LbPolicy>>),
            work_scheduler,
            pending: Mutex::default(),
        }
    }

    fn handle_resolver_update(
        self: &Arc<Self>,
        update: ResolverUpdate,
        controller: &mut InternalChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let policy_name = pick_first::POLICY_NAME;
        if let ResolverUpdate::Data(ref d) = update {
            if d.service_config.is_some() {
                return Err("can't do service configs yet".into());
            }
        } else {
            todo!("unhandled update type");
        }

        let mut p = self.policy.lock().unwrap();
        if p.is_none() {
            let builder = GLOBAL_LB_REGISTRY.get_policy(policy_name).unwrap();
            let newpol = builder.build(LbPolicyOptions {
                work_scheduler: self.clone(),
            });
            *self.policy_builder.lock().unwrap() = Some(builder);
            *p = Some(newpol);
        }

        // TODO: config should come from ServiceConfig.
        let builder = self.policy_builder.lock().unwrap();
        let config = match builder.as_ref().unwrap().parse_config(&ParsedJsonLbConfig(
            json!({"shuffleAddressList": true, "unknown_field": false}),
        )) {
            Ok(cfg) => cfg,
            Err(e) => {
                return Err(e);
            }
        };

        p.as_mut()
            .unwrap()
            .resolver_update(update, config.as_ref(), controller)

        // TODO: close old LB policy gracefully vs. drop?
    }
    pub(super) fn subchannel_update(
        &self,
        subchannel: Arc<Subchannel>,
        state: &SubchannelState,
        channel_controller: &mut dyn load_balancing::ChannelController,
    ) {
        let mut p = self.policy.lock().unwrap();

        p.as_mut()
            .unwrap()
            .subchannel_update(subchannel, state, channel_controller);
    }
}

pub(super) type WorkQueueItem = Box<dyn FnOnce(&mut InternalChannelController) + Send + Sync>;

pub struct TODO;

// Enables multiple receivers to view data output from a single producer.
// Producer calls update.  Consumers call iter() and call next() until they find
// a good value or encounter None.
pub(crate) struct Watcher<T> {
    tx: watch::Sender<Option<T>>,
    rx: watch::Receiver<Option<T>>,
}

impl<T: Clone> Watcher<T> {
    fn new() -> Self {
        let (tx, rx) = watch::channel(None);
        Self { tx, rx }
    }

    pub(crate) fn iter(&self) -> WatcherIter<T> {
        let mut rx = self.rx.clone();
        rx.mark_changed();
        WatcherIter { rx }
    }

    pub(crate) fn cur(&self) -> Option<T> {
        let mut rx = self.rx.clone();
        rx.mark_changed();
        let c = rx.borrow();
        c.clone()
    }

    fn update(&self, item: T) {
        self.tx.send(Some(item)).unwrap();
    }
}

pub(crate) struct WatcherIter<T> {
    rx: watch::Receiver<Option<T>>,
}
// TODO: Use an arc_swap::ArcSwap instead that contains T and a channel closed
// when T is updated.  Even if the channel needs a lock, the fast path becomes
// lock-free.

impl<T: Clone> WatcherIter<T> {
    /// Returns the next unseen value
    pub(crate) async fn next(&mut self) -> Option<T> {
        loop {
            self.rx.changed().await.ok()?;
            let x = self.rx.borrow_and_update();
            if x.is_some() {
                return x.clone();
            }
        }
    }
}
