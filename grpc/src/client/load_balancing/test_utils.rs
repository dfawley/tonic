use crate::client::{
    load_balancing::{ChannelController, LbState, Subchannel, SubchannelImpl, WorkScheduler},
    name_resolution::Address,
    subchannel::{ConnectivityStateWatcher, InternalSubchannel},
};
use crate::service::{Message, Request, Response, Service};
use std::{ops::Add, sync::Arc};
use tokio::{
    sync::{mpsc, Notify},
    task::AbortHandle,
};
use tonic::async_trait;

pub(crate) struct EmptyMessage {}
impl Message for EmptyMessage {}
pub(crate) fn new_request() -> Request {
    Request::new(Box::pin(tokio_stream::once(
        Box::new(EmptyMessage {}) as Box<dyn Message>
    )))
}
pub(crate) fn new_response() -> Response {
    Response::new(Box::pin(tokio_stream::once(Ok(
        Box::new(EmptyMessage {}) as Box<dyn Message>
    ))))
}

pub(crate) struct TestNopSubchannelImpl {
    address: Address,
    tx_connect: mpsc::UnboundedSender<TestEvent>,
}

impl TestNopSubchannelImpl {
    fn new(address: Address, tx_connect: mpsc::UnboundedSender<TestEvent>) -> Self {
        Self {
            address,
            tx_connect,
        }
    }
}

impl InternalSubchannel for TestNopSubchannelImpl {
    fn connect(&self, now: bool) {
        self.tx_connect
            .send(TestEvent::Connect(self.address.clone()))
            .unwrap();
    }

    fn register_connectivity_state_watcher(&self, watcher: Arc<dyn ConnectivityStateWatcher>) {}

    fn unregister_connectivity_state_watcher(&self, watcher: Arc<dyn ConnectivityStateWatcher>) {}
}

#[async_trait]
impl Service for TestNopSubchannelImpl {
    async fn call(&self, method: String, request: Request) -> Response {
        new_response()
    }
}

pub(crate) enum TestEvent {
    NewSubchannel(Address, Arc<dyn Subchannel>),
    UpdatePicker(LbState),
    RequestResolution,
    Connect(Address),
    ScheduleWork,
}

pub(crate) struct FakeChannel {
    pub tx_events: mpsc::UnboundedSender<TestEvent>,
}

impl ChannelController for FakeChannel {
    fn new_subchannel(&mut self, address: &Address) -> Arc<dyn Subchannel> {
        println!("new_subchannel called for address {}", address);
        let notify = Arc::new(Notify::new());
        let subchannel: Arc<dyn Subchannel> = Arc::new(SubchannelImpl::new(
            address.clone(),
            notify.clone(),
            Arc::new(TestNopSubchannelImpl::new(
                address.clone(),
                self.tx_events.clone(),
            )),
        ));
        self.tx_events
            .send(TestEvent::NewSubchannel(
                address.clone(),
                subchannel.clone(),
            ))
            .unwrap();
        subchannel
    }
    fn update_picker(&mut self, update: LbState) {
        self.tx_events
            .send(TestEvent::UpdatePicker(update))
            .unwrap();
    }
    fn request_resolution(&mut self) {
        self.tx_events.send(TestEvent::RequestResolution).unwrap();
    }
}

pub(crate) struct TestWorkScheduler {
    pub tx_events: mpsc::UnboundedSender<TestEvent>,
}

impl WorkScheduler for TestWorkScheduler {
    fn schedule_work(&self) {
        self.tx_events.send(TestEvent::ScheduleWork).unwrap();
    }
}
