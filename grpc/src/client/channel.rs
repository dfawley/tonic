use std::error::Error;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::vec;

use tokio::sync::Notify;
use tokio::task::AbortHandle;
use url::Url; // NOTE: http::Uri requires non-empty authority portion of URI

use crate::attributes::Attributes;
use crate::credentials::Credentials;
use crate::rt;
use crate::service::{Request, Response};

use super::load_balancing::{
    pick_first, LbPolicy, LbPolicyBuilder, LbPolicyOptions, LbPolicyRegistry, GLOBAL_LB_REGISTRY,
};
use super::name_resolution::{
    self, ResolverBuilder, ResolverOptions, ResolverRegistry, ResolverUpdate,
    GLOBAL_RESOLVER_REGISTRY,
};
use super::service_config::ParsedServiceConfig;
use super::subchannel::InternalSubchannelPool;
use super::transport::TransportRegistry;

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
            self.get_active_channel()
        };
        if let Some(s) = ac.lb.subchannel_pool.connectivity_state.cur() {
            return *s;
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

    fn get_active_channel(&self) -> Arc<ActiveChannel> {
        let mut s = self.inner.active_channel.lock().unwrap();
        if s.is_none() {
            *s = Some(Arc::new(ActiveChannel::new(self.inner.target.clone())));
        }
        s.clone().unwrap()
    }

    pub async fn call(&self, request: Request) -> Response {
        let ac = self.get_active_channel();
        ac.call(request).await
    }
}

struct PersistentChannel {
    target: Url,
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
        }
    }
}

struct ActiveChannel {
    cur_state: Mutex<ConnectivityState>,
    lb: Arc<LoadBalancer>,
    abort_handle: AbortHandle,
}

impl ActiveChannel {
    fn new(target: Url) -> Self {
        let resolve_now_notify = Arc::new(Notify::new());
        let lb = Arc::new(LoadBalancer::new(resolve_now_notify.clone()));

        let lb2 = lb.clone();
        let jh = tokio::task::spawn(async move {
            let rb = GLOBAL_RESOLVER_REGISTRY.get_scheme(target.scheme());
            let resolver =
                rb.unwrap()
                    .build(target.clone(), Box::new(lb2), ResolverOptions::default());
            loop {
                resolve_now_notify.notified().await;
                resolver.resolve_now();
            }
        });

        Self {
            cur_state: Mutex::new(ConnectivityState::Connecting),
            lb,
            abort_handle: jh.abort_handle(),
        }
    }

    async fn call(&self, request: Request) -> Response {
        // pre-pick tasks (e.g. deadlines, interceptors, retry)
        // start attempt
        // pick subchannel
        // perform attempt on transport
        self.lb.subchannel_pool.call(request).await
    }
}

impl Drop for ActiveChannel {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

// A channel that is not idle (connecting, ready, or erroring).
struct LoadBalancer {
    policy: Arc<Mutex<Option<Box<dyn LbPolicy>>>>, // TODO: replace with gsb which can be directly owned but has Mutex<Option> internally
    policy_builder: Mutex<Option<Arc<dyn LbPolicyBuilder>>>,
    subchannel_pool: Arc<InternalSubchannelPool>,
}

impl LoadBalancer {
    fn new(request_resolution: Arc<Notify>) -> Self {
        let policy = Arc::new(Mutex::new(None::<Box<dyn LbPolicy>>));

        let subchannel_pool = Arc::new(InternalSubchannelPool::new(request_resolution));

        let p2 = policy.clone();
        let sp2 = subchannel_pool.clone();
        tokio::task::spawn(async move {
            while let Some(update) = sp2.next_update().await {
                p2.lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .subchannel_update(&update);
            }
        });

        Self {
            policy_builder: Mutex::default(),
            policy,
            subchannel_pool,
        }
    }
    fn handle_resolver_update(
        &self,
        update: ResolverUpdate,
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
            let newpol = builder.build(self.subchannel_pool.clone(), LbPolicyOptions {});
            *self.policy_builder.lock().unwrap() = Some(builder);
            *p = Some(newpol);
        }

        // TODO: config should come from ParsedServiceConfig.
        let config = self
            .policy_builder
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .parse_config("");

        p.as_mut().unwrap().resolver_update(update, config)

        // TODO: close old LB policy gracefully vs. drop?
    }
}

impl name_resolution::LoadBalancer for Arc<LoadBalancer> {
    fn parse_config(
        &self,
        config: &str,
    ) -> Result<ParsedServiceConfig, Box<dyn Error + Send + Sync>> {
        Ok(ParsedServiceConfig {})
    }
    fn update(&self, state: ResolverUpdate) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.handle_resolver_update(state)
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum ConnectivityState {
    Idle,
    Connecting,
    Ready,
    TransientFailure,
}

pub struct TODO;
