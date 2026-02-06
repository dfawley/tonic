/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

use crate::client::CallOptions;
use crate::client::Invoke;
use crate::client::InvokeOnce;
use crate::client::RecvStream;
use crate::client::SendStream;

/// A trait which allows intercepting an RPC invoke operation.
pub trait Intercept<I>: Send + Sync {
    /// Intercepts the start of a call.  Implementations should generally use
    /// next to create and start a call whose streams are optionally wrapped
    /// before being returned.
    fn intercept(
        &self,
        method: String,
        args: CallOptions,
        next: I,
    ) -> (
        impl SendStream + use<Self, I>,
        impl RecvStream + use<Self, I>,
    );
}

/// A trait which allows intercepting a call one time only.
pub trait InterceptOnce<I>: Send + Sync {
    /// Starts a call.  Implementations should generally use next to create and
    /// start a call whose streams are optionally wrapped before being returned.
    fn intercept_once(
        self,
        method: String,
        args: CallOptions,
        next: I,
    ) -> (impl SendStream, impl RecvStream);
}

impl<Int: Intercept<Inv>, Inv> InterceptOnce<Inv> for &Int {
    fn intercept_once(
        self,
        method: String,
        options: CallOptions,
        next: Inv,
    ) -> (impl SendStream, impl RecvStream) {
        self.intercept(method, options, next)
    }
}

/// Wraps `Invoke` and an `Intercept` impls and implements `Invoke` for the
/// combination.
#[derive(Clone)]
pub struct Intercepted<Inv, Int> {
    invoke: Inv,
    intercept: Int,
}

impl<Inv, Int> Intercepted<Inv, Int> {
    pub fn new(invoke: Inv, intercept: Int) -> Self {
        Self { invoke, intercept }
    }
}

impl<Inv, Int> Invoke for Intercepted<Inv, Int>
where
    Inv: Invoke,
    for<'a> Int: Intercept<&'a Inv>,
{
    fn invoke(&self, method: String, options: CallOptions) -> (impl SendStream, impl RecvStream) {
        self.intercept.intercept(method, options, &self.invoke)
    }
}

impl<Inv, Int> InvokeOnce for Intercepted<Inv, Int>
where
    Inv: InvokeOnce,
    Int: Intercept<Inv>,
{
    fn invoke_once(
        self,
        method: String,
        options: CallOptions,
    ) -> (impl SendStream, impl RecvStream) {
        self.intercept.intercept(method, options, self.invoke)
    }
}

/// Wraps `InvokeOnce` and `InterceptorOnce` impls and implements `InvokeOnce`
/// for the combination.
pub struct InterceptedOnce<Inv, Int> {
    invoke: Inv,
    intercept: Int,
}

impl<Inv, Int> InterceptedOnce<Inv, Int> {
    pub fn new(invoke: Inv, intercept: Int) -> Self {
        Self { invoke, intercept }
    }
}

impl<Inv, Int> InvokeOnce for InterceptedOnce<Inv, Int>
where
    Inv: Send + Sync,
    Int: InterceptOnce<Inv>,
{
    fn invoke_once(
        self,
        method: String,
        options: CallOptions,
    ) -> (impl SendStream, impl RecvStream) {
        self.intercept.intercept_once(method, options, self.invoke)
    }
}

// Tests for Intercepted and InterceptedOnce and examples of the four types of
// interceptors, how they are implemented, and how they are combined with
// Invoke/InvokeOnce.
//
// The four different kinds of interceptors are:
//
// 1. Reusable: uses `next` once.  impl Intercept<InvokeOnce>
//
// This can be bundled in an Intercepted with an Invoke or InvokeOnce and
// produces either an Invoke or InvokeOnce depending on the invocable.
//
// 2. ResuableMulti: uses `next` multiple times.  impl Intercept<&Invoke>
//
// This can be bundled in an Intercepted with an Invoke and produces an Invoke.
//
// 3. Oneshot: uses `next` once.  impl InterceptOnce<InvokeOnce>
//
// This can be bundled in an InterceptedOnce with an Invoke or InvokeOnce and
// produces an InvokeOnce.
//
// 4. OneshotMulti: uses `next` multiple times.  impl InterceptOnce<Invoke>
//
// This can be bundled in an InterceptedOnce with an Invoke and produces an
// InvokeOnce.
//
// Examples of these are defined below.
#[cfg(test)]
mod test {
    use std::mem::forget;
    use std::ptr;

    use crate::client::interceptor::Intercept;
    use crate::client::interceptor::InterceptOnce;
    use crate::client::CallOptions;
    use crate::client::Invoke;
    use crate::client::InvokeOnce;
    use crate::client::RecvStream;
    use crate::client::SendOptions;
    use crate::client::SendStream;
    use crate::core::ClientResponseStreamItem;
    use crate::core::RecvMessage;
    use crate::core::SendMessage;

    struct Reusable;
    impl<I: InvokeOnce> Intercept<I> for Reusable {
        fn intercept(
            &self,
            method: String,
            args: CallOptions,
            next: I,
        ) -> (impl SendStream + use<I>, impl RecvStream + use<I>) {
            let (_, rx) = next.invoke_once(method, args);
            (NopStream, rx)
        }
    }

    struct ReusableMulti;
    impl<I: Invoke> Intercept<&I> for ReusableMulti {
        fn intercept<'i>(
            &self,
            method: String,
            args: CallOptions,
            next: &'i I,
        ) -> (impl SendStream + use<'i, I>, impl RecvStream + use<'i, I>) {
            RetryStreams::start(next, method, args)
        }
    }

    struct Oneshot;
    impl<I: InvokeOnce> InterceptOnce<I> for Oneshot {
        fn intercept_once(
            self,
            method: String,
            args: CallOptions,
            next: I,
        ) -> (impl SendStream, impl RecvStream) {
            let (tx, _) = next.invoke_once(method, args);
            (tx, NopStream)
        }
    }

    struct OneshotMulti;
    impl<I: Invoke> InterceptOnce<&I> for OneshotMulti {
        fn intercept_once(
            self,
            method: String,
            args: CallOptions,
            next: &I,
        ) -> (impl SendStream, impl RecvStream) {
            let (_, _) = next.invoke(method.clone(), args.clone());
            next.invoke(method, args)
        }
    }

    struct NopStream;
    impl SendStream for NopStream {
        async fn send(&mut self, _item: &dyn SendMessage, _options: SendOptions) -> Result<(), ()> {
            Ok(())
        }
    }
    impl RecvStream for NopStream {
        async fn next(
            &mut self,
            _msg: &mut dyn RecvMessage,
        ) -> crate::core::ClientResponseStreamItem {
            ClientResponseStreamItem::StreamClosed
        }
    }

    struct RetryStreams<'a, I> {
        invoker: &'a I,
    }
    impl<'a, I: Invoke> RetryStreams<'a, I> {
        fn start(
            invoker: &'a I,
            method: String,
            options: CallOptions,
        ) -> (impl SendStream + use<'a, I>, impl RecvStream + use<'a, I>) {
            let (send_stream, recv_stream) = invoker.invoke(method.clone(), options.clone());
            (
                RetrySendStream { send_stream },
                RetryRecvStream {
                    invoker,
                    method,
                    options,
                    recv_stream,
                },
            )
        }
    }
    struct RetrySendStream<S> {
        send_stream: S,
    }
    impl<S: SendStream> SendStream for RetrySendStream<S> {
        async fn send(&mut self, item: &dyn SendMessage, options: SendOptions) -> Result<(), ()> {
            Ok(())
        }
    }
    struct RetryRecvStream<'a, I, R> {
        invoker: &'a I,
        method: String,
        options: CallOptions,
        recv_stream: R,
    }
    impl<'a, I: Invoke, R: RecvStream> RecvStream for RetryRecvStream<'a, I, R> {
        async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientResponseStreamItem {
            loop {
                let resp = self.recv_stream.next(msg).await;
                if let ClientResponseStreamItem::Trailers(t) = &resp {
                    if t.status.is_err() {
                        let (send_stream, recv_stream) = self
                            .invoker
                            .invoke(self.method.clone(), self.options.clone());
                        unsafe {
                            ptr::copy_nonoverlapping(
                                &recv_stream as *const _ as *const u8,
                                &mut self.recv_stream as *mut _ as *mut u8,
                                std::mem::size_of_val(&recv_stream),
                            );
                            forget(recv_stream);
                        }
                        continue;
                    }
                }
                return resp;
            }
        }
    }

    struct ByteRecvMsg {
        data: Option<Vec<Vec<u8>>>,
    }
    impl ByteRecvMsg {
        fn new() -> Self {
            Self { data: None }
        }
    }
    impl RecvMessage for ByteRecvMsg {
        fn decode(&mut self, data: Vec<Vec<u8>>) {
            self.data = Some(data);
        }
    }
}
