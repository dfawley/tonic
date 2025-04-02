use std::any::Any;

use grpc::client::load_balancing::pick_first;
use grpc::client::transport;
use grpc::service::{Message, Request, Response, Service};
use grpc::{client::ChannelOptions, inmemory};
use tokio::sync::mpsc::error::SendError;
use tonic::async_trait;

struct Handler {}

#[derive(Debug)]
struct MyReqMessage(String);

impl Message for MyReqMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[derive(Debug)]
struct MyResMessage(String);
impl Message for MyResMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[async_trait]
impl Service for Handler {
    async fn call(&self, mut request: Request) -> Response {
        let (res, tx) = Response::new();
        tokio::task::spawn(async move {
            while let Some(req) = request.next::<MyReqMessage>().await {
                tx.send(Box::new(MyResMessage(format!(
                    "responding to: {}; msg: {}",
                    request.method, req.0,
                ))))
                .await?;
            }
            println!("no more requests on stream");
            Ok::<(), SendError<Box<dyn Message>>>(())
        });
        res
    }
}

#[tokio::main]
async fn main() {
    inmemory::reg();
    pick_first::reg();
    let lis = inmemory::Listener::new();
    let mut srv = grpc::server::Server::new();
    srv.set_handler(Handler {});
    let target = lis.target();
    let lis2 = lis.clone();
    tokio::task::spawn(async move {
        srv.serve(&lis2).await;
        println!("serve returned!");
    });

    println!("Creating channel for {target}");
    let chan_opts =
        ChannelOptions::default().transport_registry(transport::GLOBAL_TRANSPORT_REGISTRY.clone());
    let chan = grpc::client::Channel::new(target.as_str(), None, None, chan_opts);

    let (req, tx) = Request::new("hi", None);
    let mut res = chan.call(req).await;

    let _ = tx
        .send(Box::new(MyReqMessage("My Request 1".to_string())))
        .await;
    let _ = tx
        .send(Box::new(MyReqMessage("My Request 2".to_string())))
        .await;
    let _ = tx
        .send(Box::new(MyReqMessage("My Request 3".to_string())))
        .await;
    drop(tx);

    while let Some(resp) = res.next::<MyResMessage>().await {
        println!("CALL RESPONSE: {}", resp.0);
    }
    lis.close().await;
}
