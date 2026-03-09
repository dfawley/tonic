use std::any::TypeId;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

use bytes::Buf;
use bytes::Bytes;
use futures::Sink;
use futures::sink::unfold;
use grpc::Status;
use grpc::StatusCode;
use grpc::client::CallOptions;
use grpc::client::InvokeOnce;
use grpc::client::RecvStream as ClientRecvStream;
use grpc::client::SendOptions;
use grpc::client::SendStream as ClientSendStream;
use grpc::client::stream_util::RecvStreamValidator;
use grpc::core::ClientResponseStreamItem;
use grpc::core::MessageType;
use grpc::core::RecvMessage;
use grpc::core::RequestHeaders;
use grpc::core::SendMessage;
use protobuf::AsMut;
use protobuf::AsView;
use protobuf::ClearAndParse;
use protobuf::Message;
use protobuf::MessageMut;
use protobuf::MessageView;
use protobuf::MutProxied;
use protobuf::Proxied;
use protobuf::Serialize;

pub struct UnaryCallBuilder<'a, C, Res, ReqMsgView> {
    channel: C,
    method: String,
    req: ReqMsgView,
    args: CallOptions,
    _phantom: PhantomData<&'a Res>,
}

impl<'a, C, Res, ReqMsgView> UnaryCallBuilder<'a, C, Res, ReqMsgView>
where
    C: InvokeOnce,
{
    pub fn new(channel: C, method: impl ToString, req: ReqMsgView) -> Self {
        Self {
            channel,
            req,
            method: method.to_string(),
            args: Default::default(),
            _phantom: PhantomData,
        }
    }

    pub async fn with_response_message(self, res: &mut impl AsMut<MutProxied = Res>) -> Status
    where
        // ReqMsgView is a proto message view. (Ideally we could just require
        // "AsView" and protobuf would automatically include the rest.)
        ReqMsgView: AsView + Send + Sync + 'a,
        <ReqMsgView as AsView>::Proxied: Message,
        for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b>,
        // Res is a proto message. (Ideally we could just require "Message" and
        // protobuf would automatically include the rest.)
        Res: Message,
        for<'b> Res::Mut<'b>: ClearAndParse + Send + Sync,
    {
        let headers = RequestHeaders::new().with_method_name(self.method);
        let (mut tx, rx) = self.channel.invoke_once(headers, self.args).await;
        let mut rx = RecvStreamValidator::new(rx, true);
        let req = &ProtoSendMessage::from_view(&self.req);
        let _ = tx.send(req, SendOptions::new().with_final_msg(true)).await;
        let mut res = ProtoRecvMessage::from_mut(res);
        loop {
            let i = rx.next(&mut res).await;
            if let ClientResponseStreamItem::Trailers(t) = i {
                return t.status().clone();
            }
        }
    }
}

impl<'a, C, Res, ReqMsgView> IntoFuture for UnaryCallBuilder<'a, C, Res, ReqMsgView>
where
    // We can make one call on C.
    C: InvokeOnce + 'a,
    // ReqMsgView is a proto message view. (Ideally we could just require
    // "AsView" and protobuf would automatically include the rest.)
    ReqMsgView: AsView + Send + Sync + 'a,
    <ReqMsgView as AsView>::Proxied: Message,
    for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b>,
    // Res is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.)
    Res: Message,
    for<'b> Res::Mut<'b>: ClearAndParse + Send + Sync,
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

pub struct GrpcStreamingResponse<M, Rx> {
    rx: RecvStreamValidator<Rx>,
    status: Option<Status>,
    _phantom: PhantomData<M>,
}

impl<M, Rx> GrpcStreamingResponse<M, Rx>
where
    Rx: ClientRecvStream,
    M: Message,
    for<'b> M::Mut<'b>: MessageMut<'b>,
{
    fn new(rx: RecvStreamValidator<Rx>) -> Self {
        Self {
            rx,
            status: None,
            _phantom: PhantomData,
        }
    }

    pub async fn next(&mut self) -> Option<M> {
        let mut res = M::default();
        let mut res_view = ProtoRecvMessage::from_mut(&mut res);
        loop {
            let i = self.rx.next(&mut res_view).await;
            match i {
                ClientResponseStreamItem::Headers(_) => continue,
                ClientResponseStreamItem::Message(_) => {
                    drop(res_view);
                    return Some(res);
                }
                ClientResponseStreamItem::Trailers(trailers) => {
                    self.status = Some(trailers.status().clone());
                    return None;
                }
                ClientResponseStreamItem::StreamClosed => return None,
            }
        }
    }

    pub async fn status(mut self) -> Status {
        if let Some(status) = self.status.take() {
            // We encountered a status while handling a call to next.
            status
        } else {
            // Drain the stream until we find trailers.
            let mut nop_msg = NopRecvMessage;
            loop {
                let i = self.rx.next(&mut nop_msg).await;
                if let ClientResponseStreamItem::Trailers(t) = i {
                    return t.status().clone();
                }
            }
        }
    }
}

struct NopRecvMessage;

impl RecvMessage for NopRecvMessage {
    fn decode(&mut self, _data: &mut dyn Buf) -> Result<(), String> {
        Ok(())
    }
}

pub struct BidiCallBuilder<C, Req, Res> {
    channel: C,
    method: String,
    args: CallOptions,
    _phantom: PhantomData<(Req, Res)>,
}

impl<C, Req, Res> BidiCallBuilder<C, Req, Res> {
    pub fn new(channel: C, method: impl ToString) -> Self {
        Self {
            channel,
            method: method.to_string(),
            args: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<C, Req, Res> BidiCallBuilder<C, Req, Res>
where
    // We can make one call on C.
    C: InvokeOnce,
    Req: Message,
    for<'b> Req::View<'b>: MessageView<'b>,
    Res: Message + ClearAndParse,
    for<'b> Res::Mut<'b>: MessageMut<'b>,
{
    pub async fn start(
        self,
    ) -> (
        impl Sink<Req, Error = ()>,
        GrpcStreamingResponse<Res, impl ClientRecvStream>,
    ) {
        let headers = RequestHeaders::new().with_method_name(self.method);
        let (tx, rx) = self.channel.invoke_once(headers, self.args).await;
        let rx = RecvStreamValidator::new(rx, false);
        let req_sink = unfold(tx, |mut tx, req| async move {
            tx.send(&ProtoSendMessage::from_view(&req), SendOptions::default())
                .await?;
            Ok(tx)
        });
        let res_stream = GrpcStreamingResponse::new(rx);
        (req_sink, res_stream)
    }
}

mod private {
    pub(crate) trait Sealed {}
}

#[allow(private_bounds)]
pub trait CallArgs: private::Sealed {
    fn args_mut(&mut self) -> &mut CallOptions;
}

impl<'a, C, Res, ReqMsg> private::Sealed for UnaryCallBuilder<'a, C, Res, ReqMsg> {}
impl<'a, C, Res, ReqMsg> CallArgs for UnaryCallBuilder<'a, C, Res, ReqMsg> {
    fn args_mut(&mut self) -> &mut CallOptions {
        &mut self.args
    }
}

impl<C, Req, Res> private::Sealed for BidiCallBuilder<C, Req, Res> {}
impl<C, Req, Res> CallArgs for BidiCallBuilder<C, Req, Res> {
    fn args_mut(&mut self) -> &mut CallOptions {
        &mut self.args
    }
}

#[allow(private_bounds)]
pub trait SharedCall: private::Sealed {
    fn with_timeout(self, timeout: Duration) -> Self;
}

impl<T: CallArgs> SharedCall for T {
    fn with_timeout(mut self, t: Duration) -> Self {
        self.args_mut().set_deadline(Instant::now() + t);
        self
    }
}

pub struct ProtoSendMessage<'a, V: Proxied>(V::View<'a>);

impl<'a, V: Proxied> ProtoSendMessage<'a, V> {
    pub fn from_view(provider: &'a impl AsView<Proxied = V>) -> Self {
        Self(provider.as_view())
    }
}

impl<'a, V> SendMessage for ProtoSendMessage<'a, V>
where
    V: Proxied,
    V::View<'a>: Serialize + Send + Sync,
{
    fn encode(&self) -> Result<Box<dyn Buf + Send + Sync>, String> {
        Ok(Box::new(Bytes::from(
            self.0.serialize().map_err(|e| e.to_string())?,
        )))
    }

    unsafe fn _ptr_for(&self, id: TypeId) -> Option<*const ()> {
        if id != TypeId::of::<V::View<'static>>() {
            return None;
        }
        Some(&self.0 as *const _ as *const ())
    }
}

impl<'a, V: Proxied> MessageType for ProtoSendMessage<'a, V> {
    type Target<'b> = V::View<'b>;
}

pub struct ProtoRecvMessage<'a, M: MutProxied>(M::Mut<'a>);

impl<'a, M: MutProxied> ProtoRecvMessage<'a, M> {
    pub fn from_mut(provider: &'a mut impl AsMut<MutProxied = M>) -> Self {
        Self(provider.as_mut())
    }
}

impl<'a, M> RecvMessage for ProtoRecvMessage<'a, M>
where
    M: MutProxied,
    M::Mut<'a>: Send + Sync + ClearAndParse,
{
    fn decode(&mut self, buf: &mut dyn Buf) -> Result<(), String> {
        let len = buf.remaining();

        if buf.chunk().len() == len {
            self.0
                .clear_and_parse(buf.chunk())
                .map_err(|e| e.to_string())?;
            // Do we need to buf.advance(len) ?
        } else {
            let mut temp_vec = vec![0u8; len];
            buf.copy_to_slice(&mut temp_vec);
            self.0
                .clear_and_parse(&temp_vec)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    unsafe fn _ptr_for(&mut self, id: TypeId) -> Option<*mut ()> {
        if id != TypeId::of::<M::Mut<'static>>() {
            return None;
        }
        Some(&mut self.0 as *mut _ as *mut ())
    }
}

impl<'a, M: Message> MessageType for ProtoRecvMessage<'a, M> {
    type Target<'b> = M::Mut<'b>;
}
