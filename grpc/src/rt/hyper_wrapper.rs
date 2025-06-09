use hyper::rt::{bounds::Http2ClientConnExec, Executor, Timer};
use hyper_util::rt::TokioIo;
use pin_project_lite::pin_project;
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{Runtime, TcpStream};

#[derive(Clone)]
pub(crate) struct HyperCompatExec {
    pub(crate) inner: Arc<dyn Runtime>,
}

impl<F> Executor<F> for HyperCompatExec
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        self.inner.spawn(Box::pin(async {
            let r = fut.await;
        }));
    }
}

struct HyperCompatSleep {
    inner: Pin<Box<dyn super::Sleep>>,
}

impl Future for HyperCompatSleep {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

impl hyper::rt::Sleep for HyperCompatSleep {}

pub(crate) struct HyperCompatTimer {
    pub(crate) inner: Arc<dyn Runtime>,
}

impl Timer for HyperCompatTimer {
    fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn hyper::rt::Sleep>> {
        let sleep = self.inner.sleep(duration);
        Box::pin(HyperCompatSleep { inner: sleep })
    }

    fn sleep_until(&self, deadline: std::time::Instant) -> Pin<Box<dyn hyper::rt::Sleep>> {
        let now = std::time::Instant::now();
        let duration = deadline.saturating_duration_since(now);
        self.sleep(duration)
    }
}

pin_project! {
/// A wrapper to make any `TcpStream` compatible with Hyper. It implements
/// Tokio's async IO traits. Callers are expected to use
    pub struct HyperStream {
        #[pin]
        inner: Box<dyn TcpStream>,
    }
}

impl HyperStream {
    /// Creates a new `HyperStream` from a type implementing `MyTcpStream`.
    pub fn new(stream: Box<dyn TcpStream>) -> Self {
        Self { inner: stream }
    }
}

impl AsyncRead for HyperStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Delegate the poll_read call to the inner stream.
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for HyperStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl hyper::rt::Read for HyperStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let n = unsafe {
            let mut tbuf = tokio::io::ReadBuf::uninit(buf.as_mut());
            match tokio::io::AsyncRead::poll_read(self.project().inner, cx, &mut tbuf) {
                Poll::Ready(Ok(())) => tbuf.filled().len(),
                other => return other,
            }
        };

        unsafe {
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }
}

impl hyper::rt::Write for HyperStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        tokio::io::AsyncWrite::poll_shutdown(self.project().inner, cx)
    }

    fn is_write_vectored(&self) -> bool {
        tokio::io::AsyncWrite::is_write_vectored(&self.inner)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        tokio::io::AsyncWrite::poll_write_vectored(self.project().inner, cx, bufs)
    }
}
