/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

use std::any::Any;
use std::any::TypeId;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use crate::client::load_balancing::subchannel::SubscriptionManager;
use crate::client::load_balancing::subchannel::Listener;
use crate::client::load_balancing::subchannel::CancelToken;
use crate::client::subchannel::SubchannelStateHandle;

use serde::Deserialize;
use serde::Serialize;

use crate::client::load_balancing::ChannelController;
use crate::client::load_balancing::DynLbConfig;
use crate::client::load_balancing::DynLbPolicy;
use crate::client::load_balancing::LbPolicy;
use crate::client::load_balancing::LbPolicyBuilder;
use crate::client::load_balancing::LbPolicyOptions;
use crate::client::load_balancing::LbState;
use crate::client::load_balancing::ParsedJsonLbConfig;
use crate::client::load_balancing::Subchannel;
use crate::client::load_balancing::SubchannelState;
use crate::client::load_balancing::WorkData;
use crate::client::load_balancing::WorkScheduler;
use crate::client::load_balancing::subchannel::ForwardingSubchannel;
use crate::client::name_resolution::Address;
use crate::client::name_resolution::ResolverUpdate;
use crate::core::RequestHeaders;

pub(crate) fn new_request_headers() -> RequestHeaders {
    RequestHeaders::default()
}

pub(crate) struct TestSubchannel {
    address: Address,
    tx_connect: std::sync::mpsc::Sender<TestEvent>,
    callbacks: Arc<Mutex<Vec<Arc<dyn Listener<SubchannelState>>>>>,
    weak_self: Weak<TestSubchannel>,
}

impl TestSubchannel {
    pub fn new(address: Address, tx_connect: std::sync::mpsc::Sender<TestEvent>, weak_self: Weak<TestSubchannel>) -> Self {
        let callbacks = Arc::new(Mutex::new(Vec::<Arc<dyn Listener<SubchannelState>>>::new()));
        Self {
            address,
            tx_connect,
            callbacks,
            weak_self,
        }
    }

    pub fn set_state(&self, state: SubchannelState) {
        let callbacks = self.callbacks.lock().unwrap();
        for cb in &*callbacks {
            cb.on_update(state.clone());
        }
    }
}

impl SubscriptionManager for TestSubchannel {
    fn remove_subscriber(&self, listener: &Arc<dyn Listener<SubchannelState>>) {
        let mut cb_list = self.callbacks.lock().unwrap();
        cb_list.retain(|cb| !Arc::ptr_eq(cb, listener));
    }
}

impl ForwardingSubchannel for TestSubchannel {
    fn delegate(&self) -> &Arc<dyn Subchannel> {
        panic!("unsupported operation on a test subchannel");
    }

    fn address(&self) -> Address {
        self.address.clone()
    }

    fn connect(&self) {
        println!("connect called for subchannel {}", self.address);
        self.tx_connect
            .send(TestEvent::Connect(self.address.clone()))
            .unwrap();
    }
    
    fn get_attribute_dyn(&self, _id: std::any::TypeId) -> Option<&dyn std::any::Any> {
        None
    }

    fn subscribe_dyn(&self, id: TypeId, listener: Box<dyn Any + Send + Sync>) -> Result<Box<dyn CancelToken>, String> {
        if id == TypeId::of::<SubchannelState>() {
            let listener_arc = listener.downcast::<Arc<dyn Listener<SubchannelState>>>()
                .map_err(|_| "invalid listener type".to_string())?;
            let listener_arc: Arc<dyn Listener<SubchannelState>> = *listener_arc;

            let handle = SubchannelStateHandle {
                manager: self.weak_self.clone() as Weak<dyn SubscriptionManager>,
                subscription: Arc::downgrade(&listener_arc),
            };

            let mut cb_list = self.callbacks.lock().unwrap();
            cb_list.push(listener_arc);
            Ok(Box::new(handle) as Box<dyn CancelToken>)
        } else {
            Err("unsupported topic/update type".to_string())
        }
    }
}

impl Hash for TestSubchannel {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}

impl PartialEq for TestSubchannel {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for TestSubchannel {}

pub(crate) enum TestEvent {
    NewSubchannel(Arc<dyn Subchannel>),
    UpdatePicker(LbState),
    RequestResolution,
    Connect(Address),
    ScheduleWork(Option<WorkData>),
}

// TODO(easwars): Remove this and instead derive Debug.
impl Debug for TestEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewSubchannel(sc) => write!(f, "NewSubchannel({})", sc.address()),
            Self::UpdatePicker(state) => write!(f, "UpdatePicker({})", state.connectivity_state),
            Self::RequestResolution => write!(f, "RequestResolution"),
            Self::Connect(addr) => write!(f, "Connect({:?})", addr.address),
            Self::ScheduleWork(data) => write!(f, "ScheduleWork({:?})", data),
        }
    }
}

/// A test channel controller that forwards calls to a channel.  This allows
/// tests to verify when a channel controller is asked to create subchannels or
/// update the picker.
pub(crate) struct TestChannelController {
    pub(crate) tx_events: std::sync::mpsc::Sender<TestEvent>,
}

impl ChannelController for TestChannelController {
    fn new_subchannel(&mut self, address: &Address) -> (Arc<dyn Subchannel>, SubchannelState) {
        println!("new_subchannel called for address {}", address);
        let subchannel: Arc<dyn Subchannel> = Arc::new_cyclic(|weak_self| {
            TestSubchannel::new(address.clone(), self.tx_events.clone(), weak_self.clone())
        });
        self.tx_events
            .send(TestEvent::NewSubchannel(subchannel.clone()))
            .unwrap();
        (subchannel, SubchannelState::idle())
    }
    fn update_picker(&mut self, update: LbState) {
        println!("picker_update called with {}", update.connectivity_state);
        self.tx_events
            .send(TestEvent::UpdatePicker(update))
            .unwrap();
    }
    fn request_resolution(&mut self) {
        self.tx_events.send(TestEvent::RequestResolution).unwrap();
    }
}

#[derive(Debug)]
pub(crate) struct TestWorkScheduler {
    pub(crate) tx_events: std::sync::mpsc::Sender<TestEvent>,
    pub(crate) pending_work: Arc<Mutex<VecDeque<Option<WorkData>>>>,
}

impl TestWorkScheduler {
    pub fn new(tx_events: std::sync::mpsc::Sender<TestEvent>) -> Self {
        Self {
            tx_events,
            pending_work: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl WorkScheduler for TestWorkScheduler {
    fn schedule_work(&self, data: Option<WorkData>) {
        self.pending_work.lock().unwrap().push_back(data);
        self.tx_events.send(TestEvent::ScheduleWork(None)).unwrap();
    }
}

pub(crate) fn run_pending_work(
    scheduler: &Arc<TestWorkScheduler>,
    policy: &mut impl LbPolicy,
    tcc: &mut dyn ChannelController,
) {
    loop {
        let data = scheduler.pending_work.lock().unwrap().pop_front();
        if let Some(data) = data {
            policy.work(data, tcc);
        } else {
            break;
        }
    }
}

// The callback to invoke when resolver_update is invoked on the stub policy.
type ResolverUpdateFn = Arc<
    dyn Fn(
            &mut StubPolicyData,
            ResolverUpdate,
            Option<&DynLbConfig>,
            &mut dyn ChannelController,
        ) -> Result<(), String>
        + Send
        + Sync,
>;

type ExitIdleFn = Arc<dyn Fn(&mut StubPolicyData, &mut dyn ChannelController) + Send + Sync>;

type WorkFn =
    Arc<dyn Fn(&mut StubPolicyData, Option<WorkData>, &mut dyn ChannelController) + Send + Sync>;

/// This struct holds `LbPolicy` trait stub functions that tests are expected to
/// implement.
#[derive(Clone, Default)]
pub(crate) struct StubPolicyFuncs {
    pub resolver_update: Option<ResolverUpdateFn>,
    pub exit_idle: Option<ExitIdleFn>,
    pub work: Option<WorkFn>,
}

impl Debug for StubPolicyFuncs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stub funcs")
    }
}

/// Data holds test data that will be passed all to functions in PolicyFuncs
#[derive(Debug)]
pub(crate) struct StubPolicyData {
    pub lb_policy_options: LbPolicyOptions,
    pub test_data: Option<Box<dyn Any + Send + Sync>>,
}

impl StubPolicyData {
    /// Creates an instance of StubPolicyData.
    pub fn new(lb_policy_options: LbPolicyOptions) -> Self {
        Self {
            test_data: None,
            lb_policy_options,
        }
    }
}

/// The stub `LbPolicy` that calls the provided functions.
#[derive(Debug)]
pub(crate) struct StubPolicy {
    funcs: StubPolicyFuncs,
    data: StubPolicyData,
}

impl LbPolicy for StubPolicy {
    type LbConfig = DynLbConfig;

    fn resolver_update(
        &mut self,
        update: ResolverUpdate,
        config: Option<&DynLbConfig>,
        channel_controller: &mut dyn ChannelController,
    ) -> Result<(), String> {
        if let Some(f) = &mut self.funcs.resolver_update {
            return f(&mut self.data, update, config, channel_controller);
        }
        Ok(())
    }

    fn exit_idle(&mut self, channel_controller: &mut dyn ChannelController) {
        if let Some(f) = &self.funcs.exit_idle {
            f(&mut self.data, channel_controller);
        }
    }

    fn work(&mut self, data: Option<WorkData>, channel_controller: &mut dyn ChannelController) {
        if let Some(f) = &self.funcs.work {
            f(&mut self.data, data, channel_controller);
        }
    }
}

impl StubPolicy {
    pub(crate) fn new(funcs: StubPolicyFuncs, options: LbPolicyOptions) -> Self {
        Self {
            funcs,
            data: StubPolicyData::new(options),
        }
    }
}

/// StubPolicyBuilder builds a StubLbPolicy.
#[derive(Debug)]
pub(crate) struct StubPolicyBuilder {
    name: &'static str,
    funcs: StubPolicyFuncs,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct MockConfig {
    shuffle_address_list: Option<bool>,
}

impl LbPolicyBuilder for StubPolicyBuilder {
    type LbPolicy = Box<DynLbPolicy>;

    fn build(&self, options: LbPolicyOptions) -> Self::LbPolicy {
        let data = StubPolicyData::new(options);
        Box::new(StubPolicy {
            funcs: self.funcs.clone(),
            data,
        })
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn parse_config(&self, config: &ParsedJsonLbConfig) -> Result<Option<DynLbConfig>, String> {
        let cfg: MockConfig = match config.convert_to() {
            Ok(c) => c,
            Err(e) => {
                return Err(format!("failed to parse JSON config: {}", e));
            }
        };
        Ok(Some(Arc::new(cfg)))
    }
}

pub(crate) fn reg_stub_policy(name: &'static str, funcs: StubPolicyFuncs) {
    super::GLOBAL_LB_REGISTRY.add_dyn_builder(Arc::new(StubPolicyBuilder { name, funcs }))
}
