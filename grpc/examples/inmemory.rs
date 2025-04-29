use std::any::Any;

use grpc::client::load_balancing::pick_first;
use grpc::client::transport;
use grpc::service::{Message, Request, Response, Service};
use grpc::{client::ChannelOptions, inmemory};
use tokio::sync::mpsc::error::SendError;
use tonic::async_trait;

struct Handler {
    id: String,
}

#[derive(Debug)]
struct MyReqMessage(String);

impl Message for MyReqMessage {}

#[derive(Debug)]
struct MyResMessage(String);
impl Message for MyResMessage {}

#[async_trait]
impl Service for Handler {
    async fn call(&self, mut request: Request) -> Response {
        let (res, tx) = Response::new();
        let id = self.id.clone();
        tokio::task::spawn(async move {
            while let Some(req) = request.next::<MyReqMessage>().await {
                tx.send(Box::new(MyResMessage(format!(
                    "Server {}: responding to: {}; msg: {}",
                    id, request.method, req.0,
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

    // Spawn the first server.
    let lis1 = inmemory::Listener::new();
    let mut srv = grpc::server::Server::new();
    srv.set_handler(Handler { id: lis1.id() });
    let lis1_clone = lis1.clone();
    tokio::task::spawn(async move {
        srv.serve(&lis1_clone).await;
        println!("serve returned for listener 1!");
    });

    // Spawn the second server.
    let lis2 = inmemory::Listener::new();
    let mut srv = grpc::server::Server::new();
    srv.set_handler(Handler { id: lis2.id() });
    let lis2_clone = lis2.clone();
    tokio::task::spawn(async move {
        srv.serve(&lis2_clone).await;
        println!("serve returned for listener 2!");
    });

    // Spawn the third server.
    let lis3 = inmemory::Listener::new();
    let mut srv = grpc::server::Server::new();
    srv.set_handler(Handler { id: lis3.id() });
    let lis3_clone = lis3.clone();
    tokio::task::spawn(async move {
        srv.serve(&lis3_clone).await;
        println!("serve returned for listener 3!");
    });

    let target = String::from("inmemory:///dummy");
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
    lis1.close().await;
    lis2.close().await;
    lis3.close().await;
}
