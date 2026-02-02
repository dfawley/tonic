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
pub trait Intercept: Send + Sync {
    /// Intercepts the start of a call.  Implementations should generally use
    /// next to create and start a call whose streams are optionally wrapped
    /// before being returned.
    fn intercept(
        &self,
        method: impl Into<String>,
        args: CallOptions,
        next: &impl Invoke,
    ) -> (impl SendStream, impl RecvStream);
}

/// A trait which allows intercepting a call one time only.
pub trait InterceptOnce: Send + Sync {
    /// Starts a call.  Implementations should generally use next to create and
    /// start a call whose streams are optionally wrapped before being returned.
    fn intercept_once(
        self,
        method: impl Into<String>,
        args: CallOptions,
        next: impl InvokeOnce,
    ) -> (impl SendStream, impl RecvStream);
}

/// Wraps `Invoke` and an `Intercept` impls and implements `Invoke` for the
/// combination.
#[derive(Clone)]
pub struct Interceptor<Inv, Int> {
    invoke: Inv,
    intercept: Int,
}

impl<Inv, Int> Interceptor<Inv, Int> {
    pub fn new(invoke: Inv, intercept: Int) -> Self {
        Self { invoke, intercept }
    }
}

impl<Inv: Invoke, Int: Intercept> Invoke for Interceptor<Inv, Int> {
    fn invoke(
        &self,
        method: impl Into<String>,
        options: CallOptions,
    ) -> (impl SendStream, impl RecvStream) {
        self.intercept.intercept(method, options, &self.invoke)
    }
}

/// Wraps `InvokeOnce` and `InterceptorOnce` impls and implements `InvokeOnce`
/// for the combination.
pub struct InterceptorOnce<Inv, Int> {
    invoke: Inv,
    intercept: Int,
}

impl<Inv, Int> InterceptorOnce<Inv, Int> {
    pub fn new(invoke: Inv, intercept: Int) -> Self {
        Self { invoke, intercept }
    }
}

impl<Inv: InvokeOnce, Int: InterceptOnce> InvokeOnce for InterceptorOnce<Inv, Int> {
    fn invoke_once(
        self,
        method: impl Into<String>,
        options: CallOptions,
    ) -> (impl SendStream, impl RecvStream) {
        self.intercept.intercept_once(method, options, self.invoke)
    }
}
