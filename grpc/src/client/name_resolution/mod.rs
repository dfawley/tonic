use core::fmt;

use std::{
    error::Error,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};
use tokio::sync::Notify;

use tonic::async_trait;
use url::Url;

use crate::attributes::Attributes;

mod passthrough;
mod registry;
pub use registry::{ResolverRegistry, SharedResolverBuilder, GLOBAL_RESOLVER_REGISTRY};

use super::service_config::ParsedServiceConfig;

#[derive(Debug, PartialEq)]
pub struct TODO;

/// A name resolver factory
pub trait ResolverBuilder: Send + Sync {
    /// Builds a name resolver instance, or returns an error.
    fn build(
        &self,
        target: Url,
        resolve_now: Arc<Notify>,
        options: ResolverOptions,
    ) -> Box<dyn Resolver>;
    /// Reports the URI scheme handled by this name resolver.
    fn scheme(&self) -> &'static str;
    /// Returns the default authority for a channel using this name resolver and
    /// target.
    fn default_authority(&self, target: &Url) -> String {
        let path = target.path();
        path.strip_prefix("/").unwrap_or(path).to_string()
    }
}

#[async_trait]
pub trait ChannelController: Send + Sync {
    fn parse_config(
        &self,
        config: &str,
    ) -> Result<ParsedServiceConfig, Box<dyn Error + Send + Sync>>; // TODO
    async fn update(&self, update: ResolverUpdate) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub enum ResolverUpdate {
    Err(Box<dyn Error + Send + Sync>), // The name resolver encountered an error.
    Data(ResolverData),                // The name resolver produced a result.
}

#[derive(Debug, Default)]
#[non_exhaustive]
pub struct ResolverOptions {
    authority: String,
}

/// Data provided by the name resolver
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct ResolverData {
    pub endpoints: Vec<Endpoint>,
    pub service_config: Option<ParsedServiceConfig>,
    // Contains optional data which can be used by the LB Policy or channel.
    pub attributes: Attributes,
}

#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct Endpoint {
    pub addresses: Vec<Address>,
    // Contains optional data which can be used by the LB Policy.
    pub attributes: Attributes,
}

impl Eq for Endpoint {}

impl PartialEq for Endpoint {
    fn eq(&self, other: &Self) -> bool {
        self.addresses == other.addresses
    }
}

impl Hash for Endpoint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.addresses.hash(state);
    }
}

#[derive(Debug, Default, Clone)] // TODO: define manually to get type of addr
#[non_exhaustive]
pub struct Address {
    // The address a string describing its type and a string.
    pub address_type: String, // TODO: &'static str?
    pub address: String,
    // Contains optional data which can be used by the Subchannel or transport.
    pub attributes: Attributes,
}

impl Eq for Address {}

impl PartialEq for Address {
    fn eq(&self, other: &Self) -> bool {
        self.address_type == other.address_type && self.address == other.address
    }
}

impl Hash for Address {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address_type.hash(state);
        self.address.hash(state);
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.address_type, self.address)
    }
}

pub static TCP_IP_ADDRESS_TYPE: &str = "tcp";

#[async_trait]
pub trait Resolver: Send + Sync {
    async fn start(&mut self, channel_controller: Box<dyn ChannelController>);
}
