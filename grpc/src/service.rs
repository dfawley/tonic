use std::{any::Any, time::Instant};

use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::async_trait;

#[derive(Debug)]
struct TODO;

pub struct Headers {}
pub struct Trailers {}

// TODO: are requests and responses different on client and server?  or do they
// just have different extensions available? E.g. peer would be an extension
// added to the request on the server but the client wouldn't see it.  The
// stream/messages are different: the client can write request messages where
// the server can only read them, and vice-versa.

#[derive(Debug)]
pub struct Request {
    pub method: String,
    deadline: Option<Instant>,
    rx: Receiver<Box<dyn Message>>,
    // Should all of the below optional things be "extensions"?
    /*metadata: MetadataMap,
    compressor: Option<String>,
    wait_for_ready: bool,*/
}

// TODO: needs a builder to keep setters off of the constructed type.
impl Request {
    pub fn new(method: &str, parent: Option<&Request>) -> (Self, Sender<Box<dyn Message>>) {
        let (tx, rx) = mpsc::channel(1);
        (
            Self {
                method: method.to_string(),
                rx,
                deadline: parent.and_then(|p| p.deadline),
            },
            tx,
        )
    }
    pub async fn next<T: Message + 'static>(&mut self) -> Option<Box<T>> {
        (self.rx.recv().await? as Box<dyn Any>).downcast::<T>().ok()
    }
    fn headers(&mut self) -> Headers {
        Headers {}
    }
}

#[derive(Debug)]
pub struct Response {
    rx: Receiver<Box<dyn Message>>, // TODO: include headers & trailers in one stream?
}

impl Response {
    pub fn new() -> (Self, Sender<Box<dyn Message>>) {
        let (tx, rx) = mpsc::channel(1);
        (Self { rx }, tx)
    }
    pub async fn next<T: Message + 'static>(&mut self) -> Option<Box<T>> {
        (self.rx.recv().await? as Box<dyn Any>).downcast::<T>().ok()
    }
    pub async fn headers(&mut self) -> Headers {
        Headers {}
    }
    pub async fn trailers(&mut self) -> Trailers {
        Trailers {}
    }
}

// TODO: very similar to tower, obviously.  It's probably fine to always output
// a response, though, and the response will always contain the error as a grpc
// status & message.  Errors are delivered asynchronously in the case of a real
// RPC.
//
// Or can/should we use tonic's types (Service/Req/Res) directly?
#[async_trait]
pub trait Service: Send + Sync {
    async fn call(&self, request: Request) -> Response;
}

pub trait Message: Any + Send {}
