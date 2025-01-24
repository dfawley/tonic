use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::sleep;

use crate::{
    client::{
        load_balancing::LbState,
        name_resolution::{Address, ResolverUpdate},
        subchannel, ConnectivityState,
    },
    service::Request,
};

use super::{
    ChannelController, LbConfig, LbPolicyBuilderSingle, LbPolicyOptions, LbPolicySingle, Pick,
    PickResult, Picker, Subchannel, SubchannelState, WorkScheduler
};

use serde::{Deserialize, Serialize};
use serde_json;

use rand::seq::SliceRandom;
use rand;

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

    fn parse_config(&self, config: &str) -> Result<Option<LbConfig>, Box<dyn Error + Send + Sync>> {
        let cfg = match serde_json::from_str::<LbPolicyConfig>(config) {
            Ok(cfg) => cfg,
            Err(err) => {
                return Err(format!("service config parsing failed: {err}").into());
            }
        };
        let cfg = LbConfig::new(Arc::new(cfg));
        Ok(Some(cfg))
    }
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct LbPolicyConfig {
    shuffle_address_list: Option<bool>,
}

/*
impl From<LbConfig> for LbPolicyConfig {
    fn from(value: LbConfig) -> Self {
        value.config.downcast_ref::<LbPolicyConfig>()
    }
}
*/

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

        let mut shuffle_addresses = false;
        if let Some(cfg) = config {
            let cfg: &LbPolicyConfig = cfg.into().unwrap();
            if let Some(v) = cfg.shuffle_address_list {
                shuffle_addresses = v;
            }
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
        PickResult::Subchannel(Pick {
            subchannel: self.sc.clone(),
            on_complete: None,
            metadata: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::client::load_balancing::{LbConfig, LbPolicyBuilderSingle, GLOBAL_LB_REGISTRY};
    use std::sync::Arc;
    use super::*;

    #[test]
    fn pickfirst_builder_name() -> Result<(), String> {
        reg();

        let builder: Arc<dyn LbPolicyBuilderSingle> = match GLOBAL_LB_REGISTRY.get_policy("pick_first") {
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

        let builder: Arc<dyn LbPolicyBuilderSingle> = match GLOBAL_LB_REGISTRY.get_policy("pick_first") {
            Some(b) => b,
            None => {
                return Err(String::from("pick_first LB policy not registered"));
            }
        };

        // Failure cases.
        assert_eq!(builder.parse_config("").is_err(), true);
        assert_eq!(builder.parse_config("This is not JSON").is_err(), true);
        assert_eq!(builder.parse_config("{").is_err(), true);
        assert_eq!(builder.parse_config("}").is_err(), true);

        // Success cases.
        struct TestCase {
            config: String,
            want_shuffle_addresses: bool,
        }
        let test_cases = vec![
            TestCase{config: String::from("{}"), want_shuffle_addresses: false},
        ];
        for tc in test_cases {
            let config = match builder.parse_config(tc.config.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    let err = format!("parse_config({}) failed when expected to succeed: {:?}", tc.config, e).clone(); 
                    panic!("{}", err);
                }
            };
            let config: &LbPolicyConfig = match config {
                Some(c) => c.into().unwrap(),
                None => {
                    let err = format!("parse_config({}) returned None when expected to succeed", tc.config).clone(); 
                    panic!("{}", err);
                }
            };
            /*
            let lb_cfg: LbPolicyConfig = match config.into() {
                Some(c) => c.config.downcast_ref(),
                None => {
                    let err = format!("parse_config({}) returned None when expected not to", tc.config).as_str(); 
                    panic!("{}", err);
                },
            };
             */
            assert_eq!(config.shuffle_address_list.unwrap_or(false), tc.want_shuffle_addresses);
        }
        Ok(())
    }
}