use std::marker::PhantomData;

use grpc::client::CallOptions;
use grpc::client::InvokeOnce;
use grpc::client::RecvStream;
use grpc::client::SendOptions;
use grpc::client::SendStream as _;
use grpc::client::stream_util::RecvStreamValidator;
use grpc::core::RequestHeaders;
use protobuf::AsView;
use protobuf::ClearAndParse;
use protobuf::Message;
use protobuf::MessageMut;
use protobuf::MessageView;
use protobuf::Proxied;

use crate::CallBuilder;
use crate::GrpcStreamingResponse;
use crate::ProtoSendMessage;
use crate::client::private::Sealed;

/// Configures a server-streaming call for gRPC Protobuf.  Implements
/// `IntoFuture` which begins the call and resolves to a `GrpcStreamingResponse`
/// which allows for receiving response messages and the status.  Implements
/// `CallBuilder` to provide common RPC configuration methods.
pub struct ServerStreamingCallBuilder<'a, C, ReqMsgView, Res> {
    channel: C,
    method: String,
    args: CallOptions,
    req: ReqMsgView,
    _phantom: PhantomData<&'a Res>,
}

impl<'a, C, ReqMsgView, Res> ServerStreamingCallBuilder<'a, C, ReqMsgView, Res> {
    pub fn new(channel: C, method: impl ToString, req: ReqMsgView) -> Self {
        Self {
            channel,
            req,
            method: method.to_string(),
            args: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<'a, C, ReqMsgView, Res> ServerStreamingCallBuilder<'a, C, ReqMsgView, Res>
where
    C: InvokeOnce,
    // ReqMsgView is a proto message view. (Ideally we could just require
    // "AsView" and protobuf would automatically include the rest.)
    ReqMsgView: AsView + Send + Sync + 'a,
    <ReqMsgView as AsView>::Proxied: Message,
    for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b>,
    // Res is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.)
    Res: Message + ClearAndParse,
    for<'b> Res::Mut<'b>: MessageMut<'b>,
{
    pub async fn start(self) -> GrpcStreamingResponse<Res, impl RecvStream> {
        let headers = RequestHeaders::new().with_method_name(self.method);
        let (mut tx, rx) = self.channel.invoke_once(headers, self.args).await;
        let rx = RecvStreamValidator::new(rx, false);
        let req = &ProtoSendMessage::from_view(&self.req);
        let _ = tx.send(req, SendOptions::new().with_final_msg(true)).await;
        GrpcStreamingResponse::new(rx)
    }
}

impl<'a, C, ReqMsgView, Res> Sealed for ServerStreamingCallBuilder<'a, C, ReqMsgView, Res> {}
impl<'a, C: InvokeOnce, ReqMsgView, Res> CallBuilder<C>
    for ServerStreamingCallBuilder<'a, C, ReqMsgView, Res>
{
    type Builder<NewC: InvokeOnce> = ServerStreamingCallBuilder<'a, NewC, ReqMsgView, Res>;
    fn rebuild<NewC>(
        self,
        f: impl FnOnce(C) -> NewC,
    ) -> ServerStreamingCallBuilder<'a, NewC, ReqMsgView, Res> {
        ServerStreamingCallBuilder {
            channel: f(self.channel),
            method: self.method,
            req: self.req,
            args: self.args,
            _phantom: PhantomData,
        }
    }
    fn args_mut(&mut self) -> &mut CallOptions {
        &mut self.args
    }
}
