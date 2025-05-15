/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

//! Name Resolution for gRPC.
//!
//! Name Resolution is the process by which a channel's target is converted into
//! network addresses (typically IP addresses) used by the channel to connect to
//! a service.
use core::fmt;

use super::service_config::{self, ServiceConfig};
use crate::{attributes::Attributes, rt};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    hash::Hash,
    str::FromStr,
    sync::Arc,
};
use tokio::sync::Notify;

mod passthrough;
mod registry;
pub use registry::{ResolverRegistry, GLOBAL_RESOLVER_REGISTRY};

pub struct Url {
    url: url::Url,
}

impl FromStr for Url {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<url::Url>() {
            Ok(url) => Ok(Url { url }),
            Err(err) => Err(ParseError { cause: err }),
        }
    }
}

impl From<url::Url> for Url {
    fn from(url: url::Url) -> Self {
        Url { url }
    }
}

#[derive(Debug)]
pub struct ParseError {
    cause: url::ParseError,
}

impl Url {
    pub fn authority(&self) -> &str {
        self.url.authority()
    }

    pub fn host_str(&self) -> Option<&str> {
        self.url.host_str()
    }

    pub fn path(&self) -> &str {
        self.url.path()
    }
}

/// A name resolver factory
pub trait ResolverBuilder: Send + Sync {
    /// Builds a name resolver instance, or returns an error.
    ///
    /// Note that build must not fail.  Instead, an erroring Resolver may be
    /// returned that calls ChannelController.update() with an Err value.
    fn build(&self, target: &Url, options: ResolverOptions) -> Box<dyn Resolver>;

    /// Reports the URI scheme handled by this name resolver.
    fn scheme(&self) -> &str;

    /// Returns the default authority for a channel using this name resolver
    /// and target.  This is typically the same as the service's name.  By
    /// default, the default_authority method automatically returns the path
    /// portion of the target URI, with the leading prefix removed.
    fn default_authority(&self, uri: &Url) -> String {
        uri.authority().to_string()
    }

    /// Returns a bool indicating whether the input uri is valid to create a
    /// resolver.
    fn is_valid_uri(&self, uri: &Url) -> bool;
}

/// A collection of data configured on the channel that is constructing this
/// name resolver.
#[non_exhaustive]
pub struct ResolverOptions {
    /// Authority is the effective authority of the channel for which the
    /// resolver is built.
    pub authority: String,

    /// The runtime which provides utilities to do async work.
    pub runtime: Arc<dyn rt::Runtime>,

    /// A hook into the channel's work scheduler that allows the Resolver to
    /// request the ability to perform operations on the ChannelController.
    pub work_scheduler: Arc<dyn WorkScheduler>,
}

/// Used to asynchronously request a call into the Resolver's work method.
pub trait WorkScheduler: Send + Sync {
    // Schedules a call into the LbPolicy's work method.  If there is already a
    // pending work call that has not yet started, this may not schedule another
    // call.
    fn schedule_work(&self);
}

/// Resolver watches for the updates on the specified target.
/// Updates include address updates and service config updates.
pub trait Resolver: Send {
    /// Asks the resolver to obtain an updated resolver result, if
    /// applicable.
    ///
    /// This is useful for pull-based implementations to decide when to
    /// re-resolve.  However, the implementation is not required to
    /// re-resolve immediately upon receiving this call; it may instead
    /// elect to delay based on some configured minimum time between
    /// queries, to avoid hammering the name service with queries.
    ///
    /// For push-based implementations, this may be a no-op.
    fn resolve_now(&mut self);

    /// Called serially by the work scheduler to do work after the helper's
    /// schedule_work method is called.
    fn work(&mut self, channel_controller: &mut dyn ChannelController);
}

/// The `ChannelController` trait provides the resolver with functionality
/// to interact with the channel.
pub trait ChannelController: Send + Sync {
    /// Notifies the channel about the current state of the name resolver.  If
    /// an error value is returned, the name resolver should attempt to
    /// re-resolve, if possible.  The resolver is responsible for applying an
    /// appropriate backoff mechanism to avoid overloading the system or the
    /// remote resolver.
    fn update(&mut self, update: ResolverUpdate) -> Result<(), String>;

    /// Parses the provided JSON service config and returns an instance of a
    /// ParsedServiceConfig.
    fn parse_service_config(&self, config: &str) -> Result<ServiceConfig, String>;
}

#[derive(Clone)]
#[non_exhaustive]
/// ResolverUpdate contains the current Resolver state relevant to the
/// channel.
pub struct ResolverUpdate {
    /// Attributes contains arbitrary data about the resolver intended for
    /// consumption by the load balancing policy.
    pub attributes: Arc<Attributes>,

    /// Endpoints is the latest set of resolved endpoints for the target.
    pub endpoints: Result<Vec<Endpoint>, String>,

    /// service_config contains the result from parsing the latest service
    /// config.  If it is None, it indicates no service config is present or
    /// the resolver does not provide service configs.
    pub service_config: Result<Option<ServiceConfig>, String>,

    /// An optional human-readable note describing context about the
    /// resolution, to be passed along to the LB policy for inclusion in
    /// RPC failure status messages in cases where neither endpoints nor
    /// service_config has a non-OK status.  For example, a resolver that
    /// returns an empty endpoint list but a valid service config may set
    /// to this to something like "no DNS entries found for <name>".
    pub resolution_note: Option<String>,
}

impl Default for ResolverUpdate {
    fn default() -> Self {
        ResolverUpdate {
            service_config: Ok(None),
            attributes: Arc::default(),
            endpoints: Ok(Vec::default()),
            resolution_note: None,
        }
    }
}

/// An Endpoint is an address or a collection of addresses which reference one
/// logical server.  Multiple addresses may be used if there are multiple ways
/// which the server can be reached, e.g. via IPv4 and IPv6 addresses.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct Endpoint {
    /// Addresses contains a list of addresses used to access this endpoint.
    pub addresses: Vec<Address>,

    /// Attributes contains arbitrary data about this endpoint intended for
    /// consumption by the LB policy.
    pub attributes: Attributes,
}

#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialOrd, Ord)]
/// An Address is an identifier that indicates how to connect to a server.
pub struct Address {
    /// The network type is used to identify what kind of transport to create
    /// when connecting to this address.  Typically TCP_IP_ADDRESS_TYPE.
    pub network_type: String,

    /// The address itself is passed to the transport in order to create a
    /// connection to it.
    pub address: String,

    /// Attributes contains arbitrary data about this address intended for
    /// consumption by the subchannel.
    pub attributes: Attributes,
}

impl Eq for Address {}

impl PartialEq for Address {
    fn eq(&self, other: &Self) -> bool {
        self.network_type == other.network_type && self.address == other.address
    }
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

impl Hash for Address {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.network_type.hash(state);
        self.address.hash(state);
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.network_type, self.address)
    }
}

/// Indicates the address is an IPv4 or IPv6 address that should be connected to
/// via TCP/IP.
pub static TCP_IP_NETWORK_TYPE: &str = "tcp";
