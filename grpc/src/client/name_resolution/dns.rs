//! This module implements a DNS resolver to be installed as the default resolver
//! in grpc.

use std::net::{IpAddr, SocketAddr};

use url::Host;

use super::ResolverBuilder;

const DEFAULT_PORT: u16 = 443;
const DEFAULT_DNS_PORT: u16 = 53;

struct Builder {}

impl ResolverBuilder for Builder {
    fn build(
        &self,
        target: &super::Target,
        options: super::ResolverOptions,
    ) -> Box<dyn super::Resolver> {
        todo!()
    }

    fn scheme(&self) -> &str {
        "dns"
    }

    fn is_valid_uri(&self, target: &super::Target) -> bool {
        if let Err(err) = parse_endpoint_and_authority(target) {
            println!("{}", err);
            false
        } else {
            true
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
struct HostPort {
    host: Host<String>,
    port: u16,
}

#[derive(Eq, PartialEq, Debug)]
struct ParseResult {
    endpoint: HostPort,
    authority: Option<SocketAddr>,
}

fn parse_endpoint_and_authority(target: &super::Target) -> Result<ParseResult, String> {
    // Parse the endpoint.
    let endpoint = target.path();
    let endpoint = endpoint.strip_prefix("/").unwrap_or(endpoint);
    let parse_result = parse_host_port(endpoint, DEFAULT_PORT)
        .map_err(|err| format!("Failed to parse target {}: {}", target, err))?;
    let endpoint = parse_result.ok_or("Received empty endpoint host.".to_string())?;

    // Parse the authority.
    let authority = target.authority_host_port();
    if authority.is_empty() {
        return Ok(ParseResult {
            endpoint,
            authority: None,
        });
    }
    let parse_result = parse_host_port(&authority, DEFAULT_DNS_PORT)
        .map_err(|err| format!("Failed to parse DNS authority {}: {}", target, err))?;
    let Some(authority) = parse_result else {
        return Ok(ParseResult {
            endpoint,
            authority: None,
        });
    };
    let authority = match authority.host {
        Host::Ipv4(ipv4) => SocketAddr::new(IpAddr::V4(ipv4), authority.port),
        Host::Ipv6(ipv6) => SocketAddr::new(IpAddr::V6(ipv6), authority.port),
        _ => {
            return Err(format!("Received non-IP DNS authority {}", authority.host));
        }
    };
    Ok(ParseResult {
        endpoint,
        authority: Some(authority),
    })
}

/// Takes the user input string of the format "host:port" and default port,
/// returns the parsed host and port. If string doesn't specify a port, the
/// default_port is returned. If the string doesn't specify the host,
/// Result<None> is returned.
fn parse_host_port(host_and_port: &str, default_port: u16) -> Result<Option<HostPort>, String> {
    // We need to use the https scheme otherwise url::Url::parse doesn't convert
    // IP addresses to Host::Ipv4 or Host::Ipv6 if they could represent valid
    // domains.
    let url = format!("https://{}", host_and_port);
    let url = url.parse::<url::Url>().map_err(|err| err.to_string())?;
    let port = url.port().unwrap_or(default_port);
    let host = match url.host() {
        Some(host) => host,
        None => return Ok(None),
    };
    // Convert the domain to an owned string.
    let host = match host {
        Host::Domain(s) => Host::Domain(s.to_owned()),
        Host::Ipv4(ip) => Host::Ipv4(ip),
        Host::Ipv6(ip) => Host::Ipv6(ip),
    };
    Ok(Some(HostPort { host, port }))
}

#[cfg(test)]
mod test {
    use crate::client::name_resolution::{
        dns::{parse_endpoint_and_authority, HostPort},
        Target,
    };

    use super::ParseResult;

    #[test]
    pub fn target_parsing() {
        struct TestCase {
            input: &'static str,
            want_result: Result<ParseResult, String>,
        }
        let test_cases = vec![
            TestCase {
                input: "dns:///grpc.io",
                want_result: Ok(ParseResult {
                    endpoint: HostPort {
                        host: url::Host::Domain("grpc.io".to_string()),
                        port: 443,
                    },
                    authority: None,
                }),
            },
            TestCase {
                input: "dns:///grpc.io:1234",
                want_result: Ok(ParseResult {
                    endpoint: HostPort {
                        host: url::Host::Domain("grpc.io".to_string()),
                        port: 1234,
                    },
                    authority: None,
                }),
            },
            TestCase {
                input: "dns://8.8.8.8/grpc.io:1234",
                want_result: Ok(ParseResult {
                    endpoint: HostPort {
                        host: url::Host::Domain("grpc.io".to_string()),
                        port: 1234,
                    },
                    authority: Some("8.8.8.8:53".parse().unwrap()),
                }),
            },
            TestCase {
                input: "dns://8.8.8.8:5678/grpc.io:1234/abc",
                want_result: Ok(ParseResult {
                    endpoint: HostPort {
                        host: url::Host::Domain("grpc.io".to_string()),
                        port: 1234,
                    },
                    authority: Some("8.8.8.8:5678".parse().unwrap()),
                }),
            },
            TestCase {
                input: "dns://[::1]:5678/grpc.io:1234/abc",
                want_result: Ok(ParseResult {
                    endpoint: HostPort {
                        host: url::Host::Domain("grpc.io".to_string()),
                        port: 1234,
                    },
                    authority: Some("[::1]:5678".parse().unwrap()),
                }),
            },
            TestCase {
                input: "dns://[fe80::1]:5678/127.0.0.1:1234/abc",
                want_result: Ok(ParseResult {
                    endpoint: HostPort {
                        host: url::Host::Ipv4("127.0.0.1".parse().unwrap()),
                        port: 1234,
                    },
                    authority: Some("[fe80::1]:5678".parse().unwrap()),
                }),
            },
            TestCase {
                input: "dns:///[fe80::1%80]:5678/abc",
                want_result: Err("SocketAddr doesn't support IPv6 addresses with zones".to_string()),
            },
            TestCase {
                input: "dns:///:5678/abc",
                want_result: Err("Empty host with port".to_string()),
            },
            TestCase {
                input: "dns:///grpc.io:abc/abc",
                want_result: Err("Non numeric port".to_string()),
            },
            TestCase {
                input: "dns:///grpc.io:/",
                want_result: Ok(ParseResult {
                    endpoint: HostPort {
                        host: url::Host::Domain("grpc.io".to_string()),
                        port: 443,
                    },
                    authority: None,
                }),
            },
            TestCase {
                input: "dns:///:",
                want_result: Err("No host and port".to_string()),
            },
            TestCase {
                input: "dns:///[2001:db8:a0b:12f0::1",
                want_result: Err("Invalid address".to_string()),
            },
        ];

        for tc in test_cases {
            let target: Target = tc.input.parse().unwrap();
            let got = parse_endpoint_and_authority(&target);
            if got.is_err() != tc.want_result.is_err() {
                panic!(
                    "Got error {:?}, want error: {:?}",
                    got.err(),
                    tc.want_result.err()
                );
            }
            if got.is_err() {
                continue;
            }
            assert_eq!(got.unwrap(), tc.want_result.unwrap());
        }
    }
}
