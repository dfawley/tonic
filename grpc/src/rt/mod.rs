use std::{future::Future, net::SocketAddr, pin::Pin};

#[cfg(feature = "hickory_dns")]
mod hickory_resolver;

/// An abstraction over an asynchronous runtime.
///
/// The `Runtime` trait defines the core functionality required for
/// executing asynchronous tasks, creating DNS resolvers, and performing
/// time-based operations such as sleeping. It provides a uniform interface
/// that can be implemented for various async runtimes, enabling pluggable
/// and testable infrastructure.
pub trait Runtime: Send + Sync {
    /// Spawns the given asynchronous task to run in the background.
    fn spawn(
        &self,
        task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> Box<dyn TaskHandle>;

    /// Creates and returns an instance of a DNSResolver, optionally
    /// configured by the ResolverOptions struct. This method may return an
    /// error if it fails to create the DNSResolver.
    fn get_dns_resolver(&self, opts: ResolverOptions) -> Result<Box<dyn DnsResolver>, String>;

    /// Returns a future that completes after the specified duration.
    fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn Sleep>>;
}

pub trait Sleep: Send + Sync + Future<Output = ()> {}

pub trait TaskHandle: Send + Sync {
    /// Abort the associated task.
    fn abort(&self);
}

#[tonic::async_trait]
pub trait DnsResolver: Send + Sync {
    /// Resolve an address
    async fn lookup_host_name(&self, name: &str) -> Result<Vec<std::net::IpAddr>, String>;
    /// Perform a TXT record lookup.
    async fn lookup_txt(&self, name: &str) -> Result<Vec<String>, String>;
}

#[derive(Default)]
pub struct ResolverOptions {
    /// The address of the DNS server in "IP:port" format. If None, the
    /// system's default DNS server will be used.
    pub server_addr: Option<std::net::SocketAddr>,
}

pub struct SystemDnsResolver {}

#[tonic::async_trait]
impl DnsResolver for SystemDnsResolver {
    async fn lookup_host_name(&self, name: &str) -> Result<Vec<std::net::IpAddr>, String> {
        let name_with_port = match name.parse::<std::net::IpAddr>() {
            Ok(ip) => SocketAddr::new(ip, 0).to_string(),
            Err(_) => format!("{}:0", name),
        };
        let ips = tokio::net::lookup_host(name_with_port)
            .await
            .map_err(|err| err.to_string())?
            .map(|socket_addr| socket_addr.ip())
            .collect();
        Ok(ips)
    }

    async fn lookup_txt(&self, _name: &str) -> Result<Vec<String>, String> {
        Err("TXT record lookup unavailable. Enable the optional 'hickory_dns' feature to enable service config lookups.".to_string())
    }
}

struct TokioRuntime {}

impl TaskHandle for tokio::task::JoinHandle<()> {
    fn abort(&self) {
        self.abort()
    }
}

impl Sleep for tokio::time::Sleep {}

impl Runtime for TokioRuntime {
    fn spawn(
        &self,
        task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> Box<dyn TaskHandle> {
        Box::new(tokio::spawn(task))
    }

    fn get_dns_resolver(&self, opts: ResolverOptions) -> Result<Box<dyn DnsResolver>, String> {
        #[cfg(feature = "hickory_dns")]
        {
            hickory_resolver::DnsResolver::new(opts)
        }
        #[cfg(not(feature = "hickory_dns"))]
        {
            build_system_resolver(opts)
        }
    }

    fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn Sleep>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

fn build_system_resolver(opts: ResolverOptions) -> Result<Box<dyn DnsResolver>, String> {
    if opts.server_addr.is_some() {
        return Err("Custom DNS server are not supported, enable optional feature 'hickory_dns' to enable support.".to_string());
    }
    Ok(Box::new(SystemDnsResolver {}))
}

#[cfg(test)]
mod tests {
    use crate::rt::{build_system_resolver, ResolverOptions, Runtime, TokioRuntime};

    #[tokio::test]
    async fn test_lookup_hostname() {
        let runtime = TokioRuntime {};

        let dns = runtime
            .get_dns_resolver(ResolverOptions::default())
            .unwrap();
        let ips = dns.lookup_host_name("localhost").await.unwrap();
        assert_eq!(
            ips.len() > 0,
            true,
            "Expect localhost to resolve to more than 1 IPs."
        )
    }

    #[tokio::test]
    async fn test_system_resolver_txt_fails() {
        let default_resolver = build_system_resolver(ResolverOptions::default()).unwrap();

        let txt = default_resolver.lookup_txt("google.com").await;
        assert_eq!(txt.is_err(), true)
    }

    #[tokio::test]
    async fn test_system_resolver_custom_authority() {
        let opts = ResolverOptions {
            server_addr: Some("8.8.8.8:53".parse().unwrap()),
        };
        let default_resolver = build_system_resolver(opts);
        assert_eq!(default_resolver.is_err(), true)
    }
}
