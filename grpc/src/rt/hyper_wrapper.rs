use hyper::rt::{bounds::Http2ClientConnExec, Executor, Timer};
use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc};

use super::Runtime;

#[derive(Clone)]
pub(crate) struct HyperCompatExec(pub(crate) Arc<dyn Runtime>);

impl<F> Executor<F> for HyperCompatExec
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        self.0.spawn(Box::pin(async {
            let r = fut.await;
        }));
    }
}

struct HyperCompatSleep(Pin<Box<dyn super::Sleep>>);

impl Future for HyperCompatSleep {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl hyper::rt::Sleep for HyperCompatSleep {}

pub(crate) struct HyperCompatTimer<T>(pub(crate) T);

impl Timer for HyperCompatTimer<Arc<dyn Runtime>> {
    fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn hyper::rt::Sleep>> {
        let sleep = self.0.sleep(duration);
        Box::pin(HyperCompatSleep(sleep))
    }

    fn sleep_until(&self, deadline: std::time::Instant) -> Pin<Box<dyn hyper::rt::Sleep>> {
        let now = std::time::Instant::now();
        let duration = deadline.saturating_duration_since(now);
        self.sleep(duration)
    }
}
