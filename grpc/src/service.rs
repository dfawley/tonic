use std::{any::Any, time::Instant};

use futures_core::Stream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::{async_trait, Request as TonicRequest, Response as TonicResponse};

#[derive(Debug)]
struct TODO;

pub type Request = TonicRequest<Box<dyn Stream<Item = Box<dyn Message>> + Send + Sync>>;
pub type Response = TonicResponse<Box<dyn Stream<Item = Box<dyn Message>> + Send + Sync>>;

#[async_trait]
pub trait Service: Send + Sync {
    async fn call(&self, request: Request) -> Response;
}

pub trait Message: Any + Send {}
