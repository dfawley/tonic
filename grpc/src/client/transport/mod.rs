use std::{sync::Arc, time::Duration};

use crate::{rt::Runtime, service::Service};

mod registry;
mod tonic;

use ::tonic::async_trait;
pub use registry::{TransportRegistry, GLOBAL_TRANSPORT_REGISTRY};

#[async_trait]
pub trait Transport: Send + Sync {
    async fn connect(
        &self,
        config: Arc<SubchannelConfig>,
    ) -> Result<Box<dyn ConnectedTransport>, String>;
}

#[async_trait]
pub trait ConnectedTransport: Service {
    // Block until disconnected.
    async fn disconnected(&self);
}

pub(crate) struct SubchannelConfig {
    pub(crate) address: String,
    pub(crate) runtime: Arc<dyn Runtime>,
    pub(crate) init_stream_window_size: Option<u32>,
    pub(crate) init_connection_window_size: Option<u32>,
    pub(crate) http2_keep_alive_interval: Option<Duration>,
    pub(crate) http2_keep_alive_timeout: Option<Duration>,
    pub(crate) http2_keep_alive_while_idle: Option<bool>,
    pub(crate) http2_max_header_list_size: Option<u32>,
    pub(crate) http2_adaptive_window: Option<bool>,
    pub(crate) concurrency_limit: Option<usize>,
    pub(crate) rate_limit: Option<(u64, Duration)>,
    pub(crate) tcp_keepalive: Option<Duration>,
    pub(crate) tcp_nodelay: bool,
    pub(crate) connect_timeout: Option<Duration>,
}
