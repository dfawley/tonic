use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::sleep;
use tonic::metadata::MetadataMap;

use crate::{
    client::{
        load_balancing::LbState,
        name_resolution::{Address, ResolverUpdate},
        subchannel, ConnectivityState,
    },
    service::Request,
};

use super::{
    ChannelController, LbConfig, LbPolicyBuilderSingle, LbPolicyOptions, LbPolicySingle,
    ParsedJsonLbConfig, Pick, PickResult, Picker, Subchannel, SubchannelState, WorkScheduler,
};

use serde::{Deserialize, Serialize};
use serde_json::json;

use rand;
use rand::seq::SliceRandom;

pub static POLICY_NAME: &str = "pick_first";

struct Builder {}

impl LbPolicyBuilderSingle for Builder {
    fn build(&self, options: LbPolicyOptions) -> Box<dyn LbPolicySingle> {
        Box::new(PickFirstPolicy {
            work_scheduler: options.work_scheduler,
            subchannels: vec![],
            next_addresses: Vec::default(),
        })
    }

    fn name(&self) -> &'static str {
        POLICY_NAME
    }

    fn parse_config(
        &self,
        config: &ParsedJsonLbConfig,
    ) -> Result<Option<LbConfig>, Box<dyn Error + Send + Sync>> {
        let cfg: PickFirstConfig = match config.convert_to() {
            Ok(c) => c,
            Err(e) => {
                return Err(format!("failed to parse JSON config: {}", e).into());
            }
        };
        Ok(Some(LbConfig::new(cfg)))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PickFirstConfig {
    shuffle_address_list: Option<bool>,
}

pub fn reg() {
    super::GLOBAL_LB_REGISTRY.add_builder(Builder {})
}

struct PickFirstPolicy {
    work_scheduler: Arc<dyn WorkScheduler>,
    subchannels: Vec<Subchannel>,
    next_addresses: Vec<Address>,
}

impl LbPolicySingle for PickFirstPolicy {
    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&LbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let ResolverUpdate::Data(update) = update else {
            return Err("unhandled".into());
        };

        let cfg: Arc<PickFirstConfig> = config.unwrap().convert_to().expect("todo");
        let mut shuffle_addresses = false;
        if let Some(v) = cfg.shuffle_address_list {
            shuffle_addresses = v;
        }

        //let endpoints = mem::replace(&mut update.endpoints, vec![]);
        let mut addresses = update
            .endpoints
            .into_iter()
            .next()
            .ok_or("no endpoints")?
            .addresses;
        if shuffle_addresses {
            let mut rng = rand::thread_rng();
            addresses.shuffle(&mut rng);
        }

        let address = addresses.pop().ok_or("no addresses")?;

        let sc = channel_controller.new_subchannel(&address);
        self.subchannels = vec![sc.clone()];
        sc.connect();

        self.next_addresses = addresses;
        let work_scheduler = self.work_scheduler.clone();
        // TODO: Implement Drop that cancels this task.
        tokio::task::spawn(async move {
            sleep(Duration::from_millis(200)).await;
            work_scheduler.schedule_work();
        });
        // TODO: return a picker that queues RPCs.
        Ok(())
    }

    fn subchannel_update(
        &mut self,
        subchannel: &Subchannel,
        state: &SubchannelState,
        channel_controller: &mut dyn ChannelController,
    ) {
        dbg!();

        for sc in &self.subchannels {
            // Ignore updates for subchannels other than our subchannel, or if
            // the state is not Ready.
            if *sc == *subchannel && state.connectivity_state == ConnectivityState::Ready {
                channel_controller.update_picker(LbState {
                    connectivity_state: ConnectivityState::Ready,
                    picker: Arc::new(OneSubchannelPicker { sc: sc.clone() }),
                });
                break;
            }
        }
    }

    fn work(&mut self, channel_controller: &mut dyn ChannelController) {
        if let Some(address) = self.next_addresses.pop() {
            self.subchannels
                .push(channel_controller.new_subchannel(&address));
        }
    }
}

struct OneSubchannelPicker {
    sc: Subchannel,
}

impl Picker for OneSubchannelPicker {
    fn pick(&self, request: &Request) -> PickResult {
        PickResult::Pick(Pick {
            subchannel: self.sc.clone(),
            on_complete: None,
            metadata: MetadataMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::load_balancing::{LbConfig, LbPolicyBuilderSingle, GLOBAL_LB_REGISTRY};
    use std::sync::Arc;

    #[test]
    fn pickfirst_builder_name() -> Result<(), String> {
        reg();

        let builder: Arc<dyn LbPolicyBuilderSingle> =
            match GLOBAL_LB_REGISTRY.get_policy("pick_first") {
                Some(b) => b,
                None => {
                    return Err(String::from("pick_first LB policy not registered"));
                }
            };
        assert_eq!(builder.name(), "pick_first");
        Ok(())
    }

    #[test]
    fn pickfirst_builder_parse_config_failure() -> Result<(), String> {
        reg();

        let builder: Arc<dyn LbPolicyBuilderSingle> =
            match GLOBAL_LB_REGISTRY.get_policy("pick_first") {
                Some(b) => b,
                None => {
                    return Err(String::from("pick_first LB policy not registered"));
                }
            };

        // Success cases.
        struct TestCase {
            config: ParsedJsonLbConfig,
            want_shuffle_addresses: Option<bool>,
        }
        let test_cases = vec![
            TestCase {
                config: ParsedJsonLbConfig(json!({})),
                want_shuffle_addresses: None,
            },
            TestCase {
                config: ParsedJsonLbConfig(json!({"shuffleAddressList": false})),
                want_shuffle_addresses: Some(false),
            },
            TestCase {
                config: ParsedJsonLbConfig(json!({"shuffleAddressList": true})),
                want_shuffle_addresses: Some(true),
            },
            TestCase {
                config: ParsedJsonLbConfig(
                    json!({"shuffleAddressList": true, "unknownField": "foo"}),
                ),
                want_shuffle_addresses: Some(true),
            },
        ];
        for tc in test_cases {
            let config = match builder.parse_config(&tc.config) {
                Ok(c) => c,
                Err(e) => {
                    let err = format!(
                        "parse_config({:?}) failed when expected to succeed: {:?}",
                        tc.config, e
                    )
                    .clone();
                    panic!("{}", err);
                }
            };
            let config: LbConfig = match config {
                Some(c) => c,
                None => {
                    let err = format!(
                        "parse_config({:?}) returned None when expected to succeed",
                        tc.config
                    )
                    .clone();
                    panic!("{}", err);
                }
            };
            let got_config: Arc<PickFirstConfig> = config.convert_to().unwrap();
            assert_eq!(
                got_config.shuffle_address_list == tc.want_shuffle_addresses,
                true
            );
        }
        Ok(())
    }
}
