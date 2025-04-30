use std::{any::Any, error::Error, time::Instant};

use futures::stream::Stream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::{async_trait, Request as TonicRequest, Response as TonicResponse, Status};

#[derive(Debug)]
struct TODO;

pub type Request = TonicRequest<Box<dyn Stream<Item = Box<dyn Message>> + Send + Sync>>;
pub type Response =
    TonicResponse<Box<dyn Stream<Item = Result<Box<dyn Message>, Status>> + Send + Sync>>;

#[async_trait]
pub trait Service: Send + Sync {
    async fn call(&self, method: String, request: Request) -> Response;
}

pub trait Message: Any + Send + Sync {}
