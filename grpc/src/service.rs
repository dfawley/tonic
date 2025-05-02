use std::{any::Any, pin::Pin, time::Instant};

use futures_core::Stream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::{async_trait, Request as TonicRequest, Response as TonicResponse};

#[derive(Debug)]
struct TODO;

pub struct Headers {}
pub struct Trailers {}

pub type Request = TonicRequest<Pin<Box<dyn Stream<Item = Box<dyn Message>> + Send + Sync>>>;
pub type Response = TonicResponse<Pin<Box<dyn Stream<Item = Box<dyn Message>> + Send + Sync>>>;

#[async_trait]
pub trait Service: Send + Sync {
    async fn call(&self, request: Request) -> Response;
}

pub trait Message: Any + Send + Sync {}
