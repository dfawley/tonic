use std::marker::PhantomData;
use std::pin::Pin;

use grpc::Status;
use grpc::StatusCode;
use grpc::client::CallOptions;
use grpc::client::InvokeOnce;
use grpc::client::RecvStream;
use grpc::client::SendOptions;
use grpc::client::SendStream;
use grpc::client::stream_util::RecvStreamValidator;
use grpc::core::ClientResponseStreamItem;
use grpc::core::RequestHeaders;
use protobuf::AsMut;
use protobuf::AsView;
use protobuf::ClearAndParse;
use protobuf::Message;
use protobuf::MessageMut;
use protobuf::MessageView;

use crate::CallBuilder;
use crate::ProtoRecvMessage;
use crate::ProtoSendMessage;
use crate::client::private::Sealed;

/// Configures a client-streaming call for gRPC Protobuf.  Implements
/// `IntoFuture` which begins the call and resolves to a `ClientStreamingCall`
/// which allows sending request messages and receiving the response when done.
/// Implements `CallBuilder` to provide common RPC configuration methods.
pub struct ClientStreamingCallBuilder<'a, C, Req, Res> {
    channel: C,
    method: String,
    args: CallOptions,
    _phantom: PhantomData<&'a (Req, Res)>,
}

impl<'a, C, Req, Res> ClientStreamingCallBuilder<'a, C, Req, Res>
where
    C: InvokeOnce,
{
    pub fn new(channel: C, method: impl ToString) -> Self {
        Self {
            channel,
            method: method.to_string(),
            args: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<'a, C, Req, Res> IntoFuture for ClientStreamingCallBuilder<'a, C, Req, Res>
where
    C: InvokeOnce + 'a,
    // Req is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.  For now we need the
    // HRTBs.)
    Req: Message,
    for<'b> Req::View<'b>: MessageView<'b>,
    // Res is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.  For now we need the
    // HRTBs.)
    Res: Message + ClearAndParse,
    for<'b> Res::Mut<'b>: MessageMut<'b>,
{
    type Output = ClientStreamingCall<'a, C, Req, Res>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let headers = RequestHeaders::new().with_method_name(self.method);
            let (tx, rx) = self.channel.invoke_once(headers, self.args).await;

            ClientStreamingCall {
                tx,
                rx: RecvStreamValidator::new(rx, true),
                _phantom: PhantomData,
            }
        })
    }
}

impl<'a, C, Req, Res> Sealed for ClientStreamingCallBuilder<'a, C, Req, Res> {}
impl<'a, C: InvokeOnce, Req, Res> CallBuilder<C> for ClientStreamingCallBuilder<'a, C, Req, Res> {
    type Builder<NewC: InvokeOnce> = ClientStreamingCallBuilder<'a, NewC, Req, Res>;
    fn rebuild<NewC>(
        self,
        f: impl FnOnce(C) -> NewC,
    ) -> ClientStreamingCallBuilder<'a, NewC, Req, Res> {
        ClientStreamingCallBuilder {
            channel: f(self.channel),
            method: self.method,
            args: self.args,
            _phantom: PhantomData,
        }
    }
    fn args_mut(&mut self) -> &mut CallOptions {
        &mut self.args
    }
}

/// Implements the client-side logic for a client-streaming RPC.  Implements
/// `IntoFuture` which completes the call and resolves to the response as a
/// `Result<Res, Status>`.
pub struct ClientStreamingCall<'a, C: InvokeOnce, Req, Res> {
    tx: C::SendStream,
    rx: RecvStreamValidator<C::RecvStream>,
    _phantom: PhantomData<&'a (Req, Res)>,
}

impl<'a, C, Req, Res> ClientStreamingCall<'a, C, Req, Res>
where
    C: InvokeOnce + 'a,
    // Req is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.  For now we need the
    // HRTBs.)
    Req: Message,
    for<'b> Req::View<'b>: MessageView<'b>,
    // Res is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.  For now we need the
    // HRTBs.)
    Res: Message,
    for<'b> Res::Mut<'b>: MessageMut<'b>,
{
    /// Sends `message` on the stream.  Will block if flow control does not
    /// allow for sending the request message.  Returns an error if the stream
    /// has ended.  In this case, the application should call
    /// `with_response_message` or await the `ClientStreamingCall` to receive
    /// the server's response.
    ///
    /// Note: success does *not* indicate successful transmission of the request
    /// or successful receipt of the request by the server.  Success only
    /// indicates that the stream has not yet terminated.
    pub async fn send_message(&mut self, message: &impl AsView<Proxied = Req>) -> Result<(), ()> {
        let msg = ProtoSendMessage::from_view(message);
        self.tx.send(&msg, SendOptions::default()).await
    }

    /// Completes the RPC, receiving the optional response message into `res` if
    /// the returned Status is StatusCode::Ok.
    pub async fn with_response_message(self, res: &mut impl AsMut<MutProxied = Res>) -> Status {
        let Self { tx, mut rx, .. } = self;
        drop(tx);
        let mut res = ProtoRecvMessage::from_mut(res);
        loop {
            let i = rx.next(&mut res).await;
            if let ClientResponseStreamItem::Trailers(t) = i {
                return t.status().clone();
            }
        }
    }
}

impl<'a, C, Req, Res> IntoFuture for ClientStreamingCall<'a, C, Req, Res>
where
    C: InvokeOnce + 'a,
    // Req is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.  For now we need the
    // HRTBs.)
    Req: Message,
    for<'b> Req::View<'b>: MessageView<'b>,
    // Res is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.  For now we need the
    // HRTBs.)
    Res: Message,
    for<'b> Res::Mut<'b>: MessageMut<'b>,
{
    type Output = Result<Res, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut res = Res::default();
            let status = self.with_response_message(&mut res).await;
            if status.code() == StatusCode::Ok {
                Ok(res)
            } else {
                Err(status)
            }
        })
    }
}
