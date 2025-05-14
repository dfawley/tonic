use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};

pub struct DnsResolver {
    resolver: hickory_resolver::TokioResolver,
}

#[tonic::async_trait]
impl super::DnsResolver for DnsResolver {
    async fn lookup_host_name(&self, name: &str) -> Result<Vec<std::net::IpAddr>, String> {
        let response = self
            .resolver
            .lookup_ip(name)
            .await
            .map_err(|err| err.to_string())?;
        Ok(response.iter().collect())
    }

    async fn lookup_txt(&self, name: &str) -> Result<Vec<String>, String> {
        let response: Vec<_> = self
            .resolver
            .txt_lookup(name)
            .await
            .map_err(|err| err.to_string())?
            .iter()
            .map(|txt_record| {
                txt_record
                    .iter()
                    .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                    .collect::<Vec<String>>()
                    .join("")
            })
            .collect();
        Ok(response)
    }
}

impl DnsResolver {
    pub fn new(opts: super::ResolverOptions) -> Result<Box<dyn super::DnsResolver>, String> {
        let builder = if let Some(server_addr) = opts.server_addr {
            let provider = hickory_resolver::name_server::TokioConnectionProvider::default();
            let name_servers = NameServerConfigGroup::from_ips_clear(
                &[server_addr.ip()],
                server_addr.port(),
                true,
            );
            let config = ResolverConfig::from_parts(None, vec![], name_servers);
            hickory_resolver::TokioResolver::builder_with_config(config, provider)
        } else {
            hickory_resolver::TokioResolver::builder_tokio().map_err(|err| err.to_string())?
        };
        let mut resolver_opts = ResolverOpts::default();
        resolver_opts.ip_strategy = hickory_resolver::config::LookupIpStrategy::Ipv4AndIpv6;
        Ok(Box::new(DnsResolver {
            resolver: builder.with_options(resolver_opts).build(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::rt::ResolverOptions;

    #[tokio::test]
    async fn test_compare_hickory_and_default() {
        use crate::rt::build_system_resolver;

        let hickory_dns = super::DnsResolver::new(ResolverOptions::default()).unwrap();
        let mut ips_hickory = hickory_dns.lookup_host_name("localhost").await.unwrap();

        let default_resolver = build_system_resolver(ResolverOptions::default()).unwrap();

        let mut system_resolver_ips = default_resolver
            .lookup_host_name("localhost")
            .await
            .unwrap();

        // Hickory requests A and AAAA records in parallel, so the order of IPv4
        // and IPv6 addresses isn't deterministic.
        ips_hickory.sort();
        system_resolver_ips.sort();
        assert_eq!(
            ips_hickory, system_resolver_ips,
            "both resolvers should produce same IPs for localhost"
        )
    }

    #[tokio::test]
    #[ignore] // Don't run on CI as this required internet.
    async fn test_txt() {
        let hickory_dns = super::DnsResolver::new(ResolverOptions::default()).unwrap();

        let txt = hickory_dns.lookup_txt("google.com").await.unwrap();
        dbg!(&txt);
        assert_eq!(txt.len() > 1, true)
    }

    #[tokio::test]
    #[ignore] // Don't run on CI as this required internet.
    async fn test_custom_authority() {
        let opts = ResolverOptions {
            server_addr: Some("8.8.8.8:53".parse().unwrap()),
        };
        let hickory_dns = super::DnsResolver::new(opts).unwrap();
        let ips = hickory_dns.lookup_host_name("google.com").await.unwrap();
        dbg!(&ips);
        assert_eq!(ips.len() > 1, true)
    }
}
