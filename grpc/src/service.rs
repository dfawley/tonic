use std::{any::Any, pin::Pin, time::Instant};

use futures_core::Stream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::{async_trait, Request as TonicRequest, Response as TonicResponse, Status};

#[derive(Debug)]
struct TODO;

pub type Request = TonicRequest<Pin<Box<dyn Stream<Item = Box<dyn Message>> + Send + Sync>>>;
pub type Response =
    TonicResponse<Pin<Box<dyn Stream<Item = Result<Box<dyn Message>, Status>> + Send + Sync>>>;

#[async_trait]
pub trait Service: Send + Sync {
    async fn call(&mut self, method: String, request: Request) -> Result<Response, Status>;
}

pub trait Message: Any + Send + Sync {}
