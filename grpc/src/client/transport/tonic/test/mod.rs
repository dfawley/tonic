mod routeguide;
use routeguide::{
    route_guide_server::RouteGuide, Feature, Point, Rectangle, RouteNote, RouteSummary,
};
use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

use tonic::{
    codec::ProstCodec,
    transport::{server::Router, Server},
    Request, Response, Status,
};

use crate::{
    client::transport::tonic::{
        test::routeguide::route_guide_server::RouteGuideServer, SubchannelConfig,
        TonicTransportBuilder,
    },
    rt::tokio::TokioRuntime,
};

#[tokio::test]
pub async fn test_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap(); // get the assigned address
    println!("RouteGuideServer listening on: {}", addr);
    let handle = tokio::spawn(async {
        let route_guide = RouteGuideService {};

        let svc = RouteGuideServer::new(route_guide);

        let _ = Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });

    let builder = TonicTransportBuilder {};
    let config = Arc::new(SubchannelConfig {
        address: addr.to_string(),
        runtime: Arc::new(TokioRuntime {}),
        init_stream_window_size: None,
        init_connection_window_size: None,
        http2_keep_alive_interval: None,
        http2_keep_alive_timeout: None,
        http2_keep_alive_while_idle: None,
        http2_max_header_list_size: None,
        http2_adaptive_window: None,
        concurrency_limit: Some(10),
        rate_limit: None,
        tcp_keepalive: Some(Duration::from_secs(60)),
        tcp_nodelay: false,
        connect_timeout: Some(Duration::from_secs(60)),
    });
    let mut conn = builder.connect(config.clone()).await.unwrap();
    let outbound = tokio_stream::once(routeguide::RouteNote {
        location: Some(routeguide::Point {
            latitude: 12,
            longitude: 13,
        }),
        message: "Note 1".to_string(),
    });
    let mut inbound: tonic::Streaming<RouteNote> = conn
        .call(
            "/routeguide.RouteGuide/RouteChat".to_string(),
            Request::new(outbound),
            ProstCodec::default(),
        )
        .await
        .unwrap()
        .into_inner();
    while let Some(note) = inbound.message().await.unwrap() {
        println!("NOTE = {note:?}");
    }

    let outbound = tokio_stream::once(routeguide::Point {
        latitude: 12,
        longitude: 13,
    });
    let mut inbound: tonic::Streaming<Feature> = conn
        .call(
            "/routeguide.RouteGuide/GetFeature".to_string(),
            Request::new(outbound),
            ProstCodec::default(),
        )
        .await
        .unwrap()
        .into_inner();
    while let Some(feature) = inbound.message().await.unwrap() {
        println!("Feature = {feature:?}");
    }

    handle.abort();
}

#[derive(Debug)]
pub struct RouteGuideService {}

#[tonic::async_trait]
impl RouteGuide for RouteGuideService {
    async fn get_feature(&self, request: Request<Point>) -> Result<Response<Feature>, Status> {
        println!("GetFeature = {:?}", request);
        let resp = Feature {
            name: "some feature".to_string(),
            ..Default::default()
        };
        Ok(Response::new(resp))
    }

    type ListFeaturesStream = ReceiverStream<Result<Feature, Status>>;

    async fn list_features(
        &self,
        request: Request<Rectangle>,
    ) -> Result<Response<Self::ListFeaturesStream>, Status> {
        Err(Status::unimplemented("This method is not implemented"))
    }

    async fn record_route(
        &self,
        request: Request<tonic::Streaming<Point>>,
    ) -> Result<Response<RouteSummary>, Status> {
        Err(Status::unimplemented("This method is not implemented"))
    }

    type RouteChatStream = Pin<Box<dyn Stream<Item = Result<RouteNote, Status>> + Send + 'static>>;

    async fn route_chat(
        &self,
        request: Request<tonic::Streaming<RouteNote>>,
    ) -> Result<Response<Self::RouteChatStream>, Status> {
        println!("RouteChat");

        let mut notes = HashMap::new();
        let mut stream = request.into_inner();

        let output = async_stream::try_stream! {
            while let Some(note) = stream.next().await {
                let note = note?;

                let location = note.location.unwrap();

                let location_notes = notes.entry(location).or_insert(vec![]);
                location_notes.push(note);

                for note in location_notes {
                    yield note.clone();
                }
            }
        };

        Ok(Response::new(Box::pin(output) as Self::RouteChatStream))
    }
}

impl Eq for Point {}
