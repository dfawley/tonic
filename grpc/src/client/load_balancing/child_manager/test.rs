// use crate::client::{
//     load_balancing::{
//         pick_first::{
//             self, 
//         },
//         round_robin::{
//             self, 
//         },
//         child_manager::{
//             self, ChildManager,
//         },
//         test_utils::{self, FakeChannel, TestEvent, TestWorkScheduler},
//         ChannelController, ExternalSubchannel, Failing, LbConfig, LbPolicy, LbPolicyBuilder,
//         LbPolicyOptions, LbState, ParsedJsonLbConfig, PickResult, Picker, QueuingPicker,
//         Subchannel, SubchannelState, WorkScheduler, GLOBAL_LB_REGISTRY,
//     },
//     name_resolution::{Address, Endpoint, ResolverUpdate},
//     transport::{Transport, GLOBAL_TRANSPORT_REGISTRY},
//     ConnectivityState,
// };
// use crate::service::{Message, Request, Response, Service};
// use core::panic;
// use rand::{rngs::StdRng, Rng, RngCore, SeedableRng};
// use serde_json::json;
// use std::{
//     ops::Add,
//     sync::{Arc, Mutex},
// };
// use tokio::{
//     sync::{mpsc, Notify},
//     task::AbortHandle,
// };
// use tonic::async_trait;


// struct ResolverUpdateSharderStruct {builder: Arc<dyn LbPolicyBuilder>}


// impl ResolverUpdateSharder<Endpoint> for ResolverUpdateSharderStruct {
//     fn shard_update(
//         &self,
//         resolver_update: ResolverUpdate,
       
//     ) -> Result<HashMap<Endpoint, ChildUpdate>, Box<dyn Error + Send + Sync>> {
//         let mut hashmap = HashMap::new();
//         let builder = self.builder.clone();
//         for endpoint in resolver_update.endpoints.clone().unwrap().iter() {
//             println!("endpoint assigned to a child is {}", endpoint);
//             let child_update = ChildUpdate{
//                 child_policy_builder: self.builder.clone(),
//                 //create new resolver update with particular endpoint
//                 child_update: ResolverUpdate {attributes: resolver_update.attributes.clone(), endpoints: Ok(vec![endpoint.clone()]), service_config: resolver_update.service_config.clone(), resolution_note: resolver_update.resolution_note.clone()},
//             };
//             hashmap.insert(endpoint.clone(), child_update);
//         }
//         Ok(hashmap)
//     }
// }
// // Sets up the test environment.
// //
// // Performs the following:
// // 1. Creates a work scheduler.
// // 2. Creates a fake channel that acts as a channel controller.
// // 3. Creates a pick_first LB policy.
// //
// // Returns the following:
// // 1. A receiver for events initiated by the LB policy (like creating a
// //    new subchannel, sending a new picker etc).
// // 2. The LB policy to send resolver and subchannel updates from the test.
// // 3. The channel to pass to the LB policy as part of the updates.

// //this should be good
// fn setup() -> (
//     mpsc::UnboundedReceiver<TestEvent>,
//     Box<dyn LbPolicy>,
//     Box<dyn ChannelController>,
// ) {
//     pick_first::reg();
//     round_robin::reg();
    
//     let (tx_events, rx_events) = mpsc::unbounded_channel::<TestEvent>();
//     let work_scheduler = Arc::new(TestWorkScheduler {
//         tx_events: tx_events.clone(),
//     });
//     // let tcc = Box::new(FakeChannel::new(tx_events.clone()));
//     let tcc = Box::new(FakeChannel {
//         tx_events: tx_events.clone(),
//     });
//     let resolver_update_sharder = ResolverUpdateSharderStruct {builder:GLOBAL_LB_REGISTRY.get_policy("wrapped_pick_first").unwrap()};
//     let child_manager = Box::new(ChildManager::<Endpoint>::new(LbPolicyOptions { work_scheduler }, Box::new(resolver_update_sharder)));

//     (rx_events, child_manager, tcc)
// }

// fn create_endpoint_with_one_address(addr: String) -> Endpoint {
//     Endpoint {
//         addresses: vec![Address {
//             address: addr,
//             ..Default::default()
//         }],
//         ..Default::default()
//     }
// }

// // Creates a new endpoint with the specified number of addresses.
// fn create_endpoint_with_n_addresses(n: usize) -> Endpoint {
//     let mut addresses = Vec::new();
//     for i in 0..n {
//         addresses.push(Address {
//             address: format!("{}.{}.{}.{}:{}", i, i, i, i, i),
//             ..Default::default()
//         });
//     }
//     Endpoint {
//         addresses,
//         ..Default::default()
//     }
// }


// //todo
// fn create_n_endpoints_with_n_addresses(n: usize) -> Vec<Endpoint> {
//     let mut addresses = Vec::new();
//     let mut endpoints = Vec::new();
//     for i in 0..n {
//         let mut n_addresses = Vec::new();
//         for i in 0..n{
//             n_addresses.push(Address {
//                 address: format!("{}.{}.{}.{}:{}", i, i, i, i, i),
//                 ..Default::default()
//             });
//         }
//         addresses.push(n_addresses);
//     };
//     for i in 0..n{
//         endpoints.push(Endpoint {
//         addresses: addresses[i].clone(),
//         ..Default::default()
//     })
//     };
//     endpoints
// }


// //todo
// fn create_n_endpoints_with_k_addresses(n: usize, k: usize) -> Vec<Endpoint> {
//     let mut addresses = Vec::new();
//     let mut endpoints = Vec::new();
//     for i in 0..n {
//         let mut n_addresses = Vec::new();
//         for j in 0..k{
//             n_addresses.push(Address {
//                 address: format!("{}.{}.{}.{}:{}", j+i, j+i, j+i, j+i, j+i),
//                 ..Default::default()
//             });
//         }
//         addresses.push(n_addresses);
//     }
//     for i in 0..n{
//         endpoints.push(Endpoint {
//         addresses: addresses[i].clone(),
//         ..Default::default()
//     })
//     }
//     endpoints
// }


// fn create_n_endpoint_with_one_address(n: usize) -> Vec<Endpoint> {
//     let mut endpoints = Vec::new();
//     for i in 0..n {
//         let address = Address {
//             address: format!("{}.{}.{}.{}:{}", i, i, i, i, i),
//             ..Default::default()
//         };
//         endpoints.push(Endpoint {
//         addresses: vec![address.clone()],
//         ..Default::default()
//     });
//     }
//     endpoints
// }


// // Sends a resolver update to the LB policy with the specified endpoint.
// fn send_resolver_update_to_policy(
//     child_manager: &mut dyn LbPolicy,
//     endpoints: Vec<Endpoint>,
//     tcc: &mut dyn ChannelController,
// ) {
//     let update = ResolverUpdate {
//         endpoints: Ok(endpoints.clone()),
//         ..Default::default()
//     };
//     assert!(child_manager.resolver_update(update, None, tcc).is_ok());
// }


// // Sends a resolver error to the LB policy with the specified error message.

// //should be good
// fn send_resolver_error_to_policy(
//     child_manager: &mut dyn LbPolicy,
//     err: String,
//     tcc: &mut dyn ChannelController,
// ) {
//     let update = ResolverUpdate {
//         endpoints: Err(err),
//         ..Default::default()
//     };
//     assert!(child_manager.resolver_update(update, None, tcc).is_ok());
// }

// // Verifies that the subchannels are created for the given addresses in the
// // given order. Returns the subchannels created.
// async fn verify_subchannel_creation_from_policy(
//     rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
//     addresses: Vec<Address>,
// ) -> Vec<Arc<dyn Subchannel>> {
//     println!("verifying subchannel creation");
//     let mut subchannels = Vec::new();
//     for address in addresses {
//         match rx_events.recv().await.unwrap() {
//             TestEvent::NewSubchannel(addr, sc) => {
//                 assert!(addr == address.clone());
//                 subchannels.push(sc);
//             }
//             other => panic!("unexpected event {}", other),
//         };
//     }
//     subchannels
// }

// // Verifies that the subchannels are created for the given addresses in the
// // given order. Returns the subchannels created.
// async fn verify_multi_endpoint_subchannel_creation_from_policy(
//     rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
//     mut addresses: Vec<Address>,
// ) -> Vec<Arc<dyn Subchannel>> {
//     println!("verifying subchannel creation");
//     let mut subchannels = Vec::new();
//     for _ in 0..addresses.len() {
//         match rx_events.recv().await.unwrap() {
//             TestEvent::NewSubchannel(addr, sc) => {
//                 // Find and remove the address from the expected list
//                 if let Some(pos) = addresses.iter().position(|a| *a == addr) {
//                     addresses.remove(pos);
//                     subchannels.push(sc);
//                 } else {
//                     panic!("unexpected subchannel address: {:?}", addr);
//                 }
//             }
//             other => panic!("unexpected event {}", other),
//         };
//     }
//     subchannels
// }
// // Sends initial subchannel updates to the LB policy for the given
// // subchannels, with their state set to IDLE.
// fn send_initial_subchannel_updates_to_policy(
//     child_manager: &mut dyn LbPolicy,
//     subchannels: &[Arc<dyn Subchannel>],
//     tcc: &mut dyn ChannelController,
// ) {
//     for sc in subchannels {
//         child_manager.subchannel_update(sc.clone(), &SubchannelState::default(), tcc);
//     }
// }

// fn move_subchannel_to_idle(
//     child_manager: &mut dyn LbPolicy,
//     subchannel: Arc<dyn Subchannel>,
//     tcc: &mut dyn ChannelController,
// ) {
//     child_manager.subchannel_update(
//         subchannel.clone(),
//         &SubchannelState {
//             connectivity_state: ConnectivityState::Idle,
//             ..Default::default()
//         },
//         tcc,
//     );
// }

// fn move_subchannel_to_connecting(
//     child_manager: &mut dyn LbPolicy,
//     subchannel: Arc<dyn Subchannel>,
//     tcc: &mut dyn ChannelController,
// ) {
//     child_manager.subchannel_update(
//         subchannel.clone(),
//         &SubchannelState {
//             connectivity_state: ConnectivityState::Connecting,
//             ..Default::default()
//         },
//         tcc,
//     );
// }

// fn move_subchannel_to_ready(
//     child_manager: &mut dyn LbPolicy,
//     subchannel: Arc<dyn Subchannel>,
//     tcc: &mut dyn ChannelController,
// ) {
//     child_manager.subchannel_update(
//         subchannel.clone(),
//         &SubchannelState {
//             connectivity_state: ConnectivityState::Ready,
//             ..Default::default()
//         },
//         tcc,
//     );
// }

// fn move_subchannel_to_transient_failure(
//     child_manager: &mut dyn LbPolicy,
//     subchannel: Arc<dyn Subchannel>,
//     err: &str,
//     tcc: &mut dyn ChannelController,
// ) {
//     child_manager.subchannel_update(
//         subchannel.clone(),
//         &SubchannelState {
//             connectivity_state: ConnectivityState::TransientFailure,
//             last_connection_error: Some(Arc::from(Box::from(err.to_owned()))),
//         },
//         tcc,
//     );
// }

// // Verifies that a connection attempt is made to the given subchannel.
// async fn verify_connection_attempt_from_policy(
//     rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
//     subchannel: Arc<dyn Subchannel>,
// ) {
//     println!("Verify connection attempt");

//     let event = rx_events.recv().await.unwrap();
//     println!("  Subchannel: {}", event);
//     match event {
//     // match rx_events.recv().await.unwrap() {
//         TestEvent::Connect(addr) => {
//             assert!(addr == subchannel.address());
//         }
//         other => panic!("unexpected event {}", other),
//     };
   
// }

// // Verifies that a call to schedule_work is made by the LB policy.
// async fn verify_schedule_work_from_policy(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
//     println!("verify schedule work from policy");

//     match rx_events.recv().await.unwrap() {
//         TestEvent::ScheduleWork => {}
//         other => panic!("unexpected event {}", other),
//     };
// }


// // Verifies that the channel moves to CONNECTING state with a queuing picker.
// //
// // Returns the picker for tests to make more picks, if required.
// async fn verify_connecting_picker_from_policy(
//     rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
// ) -> Arc<dyn Picker> {
//     println!("verify connecting picker");
//     match rx_events.recv().await.unwrap() {
 
//         TestEvent::UpdatePicker(update) => {
//             println!("connectivity state is {}", update.connectivity_state);
//             assert!(update.connectivity_state == ConnectivityState::Connecting);
//             let req = test_utils::new_request();
//             assert!(update.picker.pick(&req) == PickResult::Queue);
//             return update.picker.clone();
//         }
//         other => panic!("unexpected event {}", other),
//     }
    
// }

// // Verifies that the channel moves to READY state with a picker that returns the
// // given subchannel.
// //
// // Returns the picker for tests to make more picks, if required.

// //need to update this? only takes care of one subchannel
// async fn verify_ready_picker_from_policy(
//     rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
//     subchannel: Arc<dyn Subchannel>,
// ) -> Arc<dyn Picker> {
//     println!("verify ready picker");
//     match rx_events.recv().await.unwrap() {
//         TestEvent::UpdatePicker(update) => {
//             println!("connectivity state for ready picker is {}", update.connectivity_state);
//             assert!(update.connectivity_state == ConnectivityState::Ready);
//             let req = test_utils::new_request();
//             match update.picker.pick(&req) {
//                 PickResult::Pick(pick) => {
//                     assert!(pick.subchannel == subchannel.clone());
//                     update.picker.clone()
//                 }
//                 other => panic!("unexpected pick result {}", other),
//             }
//         }
//         other => panic!("unexpected event {}", other),
//     }
// }



// // Verifies that the channel moves to TRANSIENT_FAILURE state with a picker
// // that returns an error with the given message. The error code should be
// // UNAVAILABLE..
// //
// // Returns the picker for tests to make more picks, if required.
// async fn verify_transient_failure_picker_from_policy(
//     rx_events: &mut mpsc::UnboundedReceiver<TestEvent>,
//     want_error: String,
// ) -> Arc<dyn Picker> {
//     let picker = match rx_events.recv().await.unwrap() {
//         TestEvent::UpdatePicker(update) => {
//             assert!(update.connectivity_state == ConnectivityState::TransientFailure);
//             let req = test_utils::new_request();
//             match update.picker.pick(&req) {
//                 PickResult::Fail(status) => {
//                     assert!(status.code() == tonic::Code::Unavailable);
//                     // assert!(status.message().contains(&want_error));
//                     update.picker.clone()
//                 }
//                 other => panic!("unexpected pick result {}", other),
//             }
//         }
//         other => panic!("unexpected event {}", other),
//     };
//     match rx_events.recv().await.unwrap() {
//         TestEvent::RequestResolution => {}
//         _ => panic!("no re-resolution requested after moving to transient_failure"),
//     }
//     picker

// }

// // Verifies that the channel moves to IDLE state.
// async fn verify_channel_moves_to_idle(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
//     match rx_events.recv().await.unwrap() {
//         TestEvent::UpdatePicker(update) => {
//             assert!(update.connectivity_state == ConnectivityState::Idle);
//         }
//         other => panic!("unexpected event {}", other),
//     };
// }

// // Verifies that the LB policy requests re-resolution.
// async fn verify_resolution_request(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
//     println!("verifying resolution request");
//     match rx_events.recv().await.unwrap() {
//         TestEvent::RequestResolution => {}
//         other => panic!("unexpected event {}", other),
//     };
// }

// const DEFAULT_TEST_SHORT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

// async fn verify_no_activity_from_policy(rx_events: &mut mpsc::UnboundedReceiver<TestEvent>) {
//     tokio::select! {
//         _ = tokio::time::sleep(DEFAULT_TEST_SHORT_TIMEOUT) => {}
//         event = rx_events.recv() => {
//             panic!("unexpected event {}", event.unwrap());
//         }
//     }
// }

// // Tests the scenario where the resolver returns an error before a valid update.
// // The LB policy should move to TRANSIENT_FAILURE state with a failing picker.
// #[tokio::test]
// async fn roundrobin_resolver_error_before_a_valid_update() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let resolver_error = String::from("resolver error");
//     send_resolver_error_to_policy(child_manager, resolver_error.clone(), tcc);
//     verify_transient_failure_picker_from_policy(&mut rx_events, resolver_error).await;
// }

// // Tests the scenario where the resolver returns an error after a valid update
// // and the LB policy has moved to READY. The LB policy should ignore the error
// // and continue using the previously received update.
// #[tokio::test]
// async fn roundrobin_resolver_error_after_a_valid_update_in_ready() {
//     //TODO: think about whether to use a bool for the general thing or not
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let endpoint = create_endpoint_with_n_addresses(2);
//     send_resolver_update_to_policy(child_manager, vec![endpoint.clone()], tcc);
//     let failed = true;
//     //iterate through endpoints here add it to a vec of vecs
//     // for endpoint in &endpoints{
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);

    

//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;

    

//     move_subchannel_to_ready(child_manager, subchannels[0].clone(), tcc);

//     println!("verrifying ready picker");
//     let picker = verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     let picker = verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;


//     println!("resolver error");
//     let resolver_error = String::from("resolver error");
//     send_resolver_error_to_policy(child_manager, resolver_error.clone(), tcc);

//     verify_no_activity_from_policy(&mut rx_events).await;

//     let req = test_utils::new_request();
//     match picker.pick(&req) {
//         PickResult::Pick(pick) => {
//             assert!(pick.subchannel == subchannels[0].clone());
//         }
//         other => panic!("unexpected pick result {}", other),
//     }
// }


// //look at either go or java and both and see the list of tests that they have...
// //come up with that list and start writing tests.

// //first few tests are hard

// //

// // Tests the scenario where the resolver returns an error after a valid update
// // and the LB policy is still trying to connect. The LB policy should ignore the
// // error and continue using the previously received update.

// //use debug macro

// #[tokio::test]
// async fn roundrobin_resolver_error_after_a_valid_update_in_connecting() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let endpoint = create_endpoint_with_n_addresses(2);
//     println!("created endpoint");
//     send_resolver_update_to_policy(child_manager, vec![endpoint.clone()], tcc);
//     println!("send_resolver_update_to_policy");
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
//     println!("printing subchannels created in the test");
//     for sc in &subchannels {
//         println!("  Subchannel: {}", sc);
//     }
//     println!("verifying subchannel creation");
//     println!("calling send_initial_subchannel_updates_to_policy");

//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);
//     // verify_connecting_picker_from_policy(&mut rx_events).await;
//     println!("send_initial_subchannel_updates_to_policy");
//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
//     let picker = verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;
    
    
//     // verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     println!("verify_connection_attempt_from_policy");
    

    

//     println!("move_subchannel_to_connecting");
//     //this is not happening... need to fix this so either child manager or child manager
//     println!("verify_connecting_picker_from_policy");
//     // verify_connecting_picker_from_policy(&mut rx_events).await;
//     //no capture with cargo test
//     let resolver_error = String::from("resolver error");

//     println!("send resolver error");
//     send_resolver_error_to_policy(child_manager, resolver_error.clone(), tcc);

//     println!("verify no activity from policy");
//     verify_no_activity_from_policy(&mut rx_events).await;

//     println!("check pick result");
//     let req = test_utils::new_request();
//     match picker.pick(&req) {
//         PickResult::Queue => {}
//         other => panic!("unexpected pick result {}", other),
//     }
// }



// // Tests the scenario where the resolver returns an error after a valid update
// // and the LB policy has moved to TRANSIENT_FAILURE after attemting to connect
// // to all addresses.  The LB policy should send a new picker that returns the
// // error from the resolver.
// // #[tokio::test]
// // async fn roundrobin_resolver_error_after_a_valid_update_in_tf() {
// //     let (mut rx_events, mut child_manager, mut tcc) = setup();
// //     let child_manager = child_manager.as_mut();
// //     let tcc = tcc.as_mut();

// //     let endpoint = create_endpoint_with_n_addresses(2);
// //     send_resolver_update_to_policy(child_manager, vec![endpoint.clone()], tcc);
// //     let subchannels =
// //         verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

// //     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);
// //     verify_connecting_picker_from_policy(&mut rx_events).await;
// //     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    
    
// //     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
// //     verify_connecting_picker_from_policy(&mut rx_events).await;
    

// //     let connection_error = String::from("test connection error");
// //     move_subchannel_to_transient_failure(child_manager, subchannels[0].clone(), &connection_error, tcc);
    
// //     move_subchannel_to_connecting(child_manager, subchannels[1].clone(), tcc);
// //     // verify_connecting_picker_from_policy(&mut rx_events).await;
// //     verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;
    
// //     move_subchannel_to_transient_failure(child_manager, subchannels[1].clone(), &connection_error, tcc);
// //     verify_transient_failure_picker_from_policy(&mut rx_events, connection_error).await;

// //     let resolver_error = String::from("resolver error");
// //     send_resolver_error_to_policy(child_manager, resolver_error.clone(), tcc);
// //     verify_transient_failure_picker_from_policy(&mut rx_events, resolver_error).await;
// // }
// // Tests the scenario where the resolver returns an update with no addresses
// // (before sending any valid update). The LB policy should move to
// // TRANSIENT_FAILURE state with a failing picker.

// //hi, consider creating a function where there are n endpoints with j addresses to test for
// //having endpoints but with 0 addresses

// #[tokio::test]
// async fn roundrobin_simple_test() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();


//     let endpoint = create_endpoint_with_n_addresses(2);
//     send_resolver_update_to_policy(child_manager, vec![endpoint.clone()], tcc);
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);
//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     move_subchannel_to_connecting(child_manager, subchannels[1].clone(), tcc);
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;

//     move_subchannel_to_ready(child_manager, subchannels[0].clone(), tcc);
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
    
// }

// #[tokio::test]
// async fn roundrobin_logic_simple_test() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();


//     let endpoints = create_n_endpoints_with_k_addresses(2, 1);
//     send_resolver_update_to_policy(child_manager, endpoints.clone(), tcc);
    
//     let first_addresses: Vec<_> = endpoints[0].addresses.clone();
//     let second_addresses: Vec<_> = endpoints[1].addresses.clone();
//     // verify_connecting_picker_from_policy(&mut rx_events).await;
//     let first_subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, first_addresses.clone()).await;
//     let second_subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, second_addresses.clone()).await;

//     send_initial_subchannel_updates_to_policy(child_manager, &first_subchannels, tcc);

//     verify_connecting_picker_from_policy(&mut rx_events).await;

//     // for sc in &subchannels {
//     //     move_subchannel_to_connecting(child_manager, sc.clone(), tcc);
//     //     verify_connection_attempt_from_policy(&mut rx_events, sc.clone()).await;
//     // }

//     // move_subchannel_to_ready(child_manager, subchannels[0].clone(), tcc);
//     // verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

//     // for sc in subchannels.iter().skip(1) {
//     //     move_subchannel_to_ready(child_manager, sc.clone(), tcc);
//     //     verify_ready_picker_from_policy(&mut rx_events, sc.clone()).await;
//     // }
    
// }

// #[tokio::test]
// async fn roundrobin_zero_addresses_from_resolver_before_valid_update() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();


//     let endpoints = create_n_endpoints_with_k_addresses(4, 0);
//     let update = ResolverUpdate {
//         endpoints: Ok(endpoints.clone()),
//         ..Default::default()
//     };
//     assert!(child_manager.resolver_update(update, None, tcc).is_err());
//     verify_transient_failure_picker_from_policy(
//         &mut rx_events,
//         "received empty address list from the name resolver".to_string(),
//     )
//     .await;
// }

// #[tokio::test]
// async fn roundrobin_stay_transient_failure_until_ready() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let endpoint = create_endpoint_with_n_addresses(2);
//     send_resolver_update_to_policy(child_manager, vec![endpoint.clone()], tcc);
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;
//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);
    
//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
    
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;
    
    
    
//     move_subchannel_to_connecting(child_manager, subchannels[1].clone(), tcc);
//     let first_error = String::from("test connection error 1");
//     for sc in &subchannels {
//         move_subchannel_to_transient_failure(child_manager, sc.clone(), &first_error, tcc);
        

//     }

 
//     println!("verifying transient failure picker");
    
//     verify_transient_failure_picker_from_policy(&mut rx_events, first_error.clone()).await;
//     println!("verifying resolution request");
//     // verify_transient_failure_picker_from_policy(&mut rx_events, first_error.clone()).await;

    
//     println!("moving subchannel to ready");
//     move_subchannel_to_ready(child_manager, subchannels[0].clone(), tcc);
   



//     println!("verifying subchannel to ready");
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
// }
// // Tests the scenario where the resolver returns an update with no endpoints
// // (before sending any valid update). The LB policy should move to
// // TRANSIENT_FAILURE state with a failing picker.

// //should be good to go
// #[tokio::test]
// async fn roundrobin_zero_endpoints_from_resolver_before_valid_update() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let update = ResolverUpdate {
//         endpoints: Ok(vec![]),
//         ..Default::default()
//     };
//     assert!(child_manager.resolver_update(update, None, tcc).is_err());
//     verify_transient_failure_picker_from_policy(
//         &mut rx_events,
//         "received empty address list from the name resolver".to_string(),
//     )
//     .await;
// }

// // Tests the scenario where the resolver returns an update with no endpoints
// // after sending a valid update (and the LB policy has moved to READY). The LB
// // policy should move to TRANSIENT_FAILURE state with a failing picker.
// #[tokio::test]
// async fn roundrobin_zero_endpoints_from_resolver_after_valid_update() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let endpoint = create_endpoint_with_n_addresses(2);
//     send_resolver_update_to_policy(child_manager, vec![endpoint.clone()], tcc);
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);
    
//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
   

//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;

    

//     move_subchannel_to_ready(child_manager, subchannels[0].clone(), tcc);

//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

//     let update = ResolverUpdate {
//         endpoints: Ok(vec![]),
//         ..Default::default()
//     };
//     assert!(child_manager.resolver_update(update, None, tcc).is_err());
//     verify_transient_failure_picker_from_policy(
//         &mut rx_events,
//         "received empty address list from the name resolver".to_string(),
//     )
//     .await;
// }

// // Tests the scenario where the resolver returns an update with one address. The
// // LB policy should create a subchannel for that address, connect to it, and
// // once the connection succeeds, move to READY state with a picker that returns
// // that subchannel.
// #[tokio::test]
// async fn roundrobin_with_one_backend() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let endpoint = create_endpoint_with_n_addresses(1);
//     send_resolver_update_to_policy(child_manager, vec![endpoint.clone()], tcc);
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);

   
//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;

//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;

    

//     move_subchannel_to_ready(child_manager, subchannels[0].clone(), tcc);
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
// }

// // Tests the scenario where the resolver returns an update with multiple
// // address. The LB policy should create subchannels for all address, and attempt
// // to connect to them in order, until a connection succeeds, at which point it
// // should move to READY state with a picker that returns that subchannel.
// #[tokio::test]
// async fn roundrobin_with_multiple_backends_first_backend_is_ready() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let endpoint = create_endpoint_with_n_addresses(2);
//     send_resolver_update_to_policy(child_manager, vec![endpoint.clone()], tcc);
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoint.addresses.clone()).await;

//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);

    


//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
    

//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;

//     move_subchannel_to_ready(child_manager, subchannels[0].clone(), tcc);

//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
// }



// // Tests the scenario where the resolver returns an update with multiple
// // address, some of which are duplicates. The LB policy should dedup the
// // addresses and create subchannels for them, and attempt to connect to them in
// // order, until a connection succeeds, at which point it should move to READY
// // state with a picker that returns that subchannel.
// #[tokio::test]
// async fn roundrobin_with_multiple_backends_duplicate_addresses() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let endpoint = Endpoint {
//         addresses: vec![
//             Address {
//                 address: format!("{}.{}.{}.{}:{}", 0, 0, 0, 0, 0),
//                 ..Default::default()
//             },
//             Address {
//                 address: format!("{}.{}.{}.{}:{}", 0, 0, 0, 0, 0),
//                 ..Default::default()
//             },
//             Address {
//                 address: format!("{}.{}.{}.{}:{}", 1, 1, 1, 1, 1),
//                 ..Default::default()
//             },
//         ],
//         ..Default::default()
//     };
//     let endpoint_with_duplicates_removed = Endpoint {
//         addresses: vec![
//             Address {
//                 address: format!("{}.{}.{}.{}:{}", 0, 0, 0, 0, 0),
//                 ..Default::default()
//             },
//             Address {
//                 address: format!("{}.{}.{}.{}:{}", 1, 1, 1, 1, 1),
//                 ..Default::default()
//             },
//         ],
//         ..Default::default()
//     };

//     send_resolver_update_to_policy(child_manager, vec![endpoint.clone()], tcc);
//     let subchannels = verify_subchannel_creation_from_policy(
//         &mut rx_events,
//         endpoint_with_duplicates_removed.addresses.clone(),
//     )
//     .await;

//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);
    
//     let connection_error = String::from("test connection error");
//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     move_subchannel_to_transient_failure(child_manager, subchannels[0].clone(), &connection_error, tcc);

//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[1].clone()).await;
//     move_subchannel_to_connecting(child_manager, subchannels[1].clone(), tcc);
//     move_subchannel_to_ready(child_manager, subchannels[1].clone(), tcc);

//     verify_ready_picker_from_policy(&mut rx_events, subchannels[1].clone()).await;
// }




// // Tests the scenario where the resolver returns an update with multiple
// // addresses and the LB policy successfully connects to first one and moves to
// // READY. The resolver then returns an update with a new address list that
// // contains the address of the currently connected subchannel. The LB policy
// // should create subchannels for the new addresses, and then see that the
// // currently connected subchannel is in the new address list. It should then
// // send a new READY picker that returns the currently connected subchannel.
// #[tokio::test]
// async fn roundrobin_resolver_update_contains_currently_ready_subchannel() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let endpoints = create_endpoint_with_n_addresses(2);
//     send_resolver_update_to_policy(child_manager, vec![endpoints.clone()], tcc);
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);
    
    
//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;
    
//     move_subchannel_to_ready(child_manager, subchannels[0].clone(), tcc);

//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

//     let mut endpoints = create_endpoint_with_n_addresses(4);
//     endpoints.addresses.reverse();
//     send_resolver_update_to_policy(child_manager, vec![endpoints.clone()], tcc);
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
//     child_manager.subchannel_update(subchannels[0].clone(), &SubchannelState::default(), tcc);
//     verify_connecting_picker_from_policy(&mut rx_events).await;

//     child_manager.subchannel_update(subchannels[1].clone(), &SubchannelState::default(), tcc);

//     child_manager.subchannel_update(subchannels[2].clone(), &SubchannelState::default(), tcc);
//     child_manager.subchannel_update(
//         subchannels[3].clone(),
//         &SubchannelState {
//             connectivity_state: ConnectivityState::Ready,
//             ..Default::default()
//         },
//         tcc,
//     );
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[3].clone()).await;
// }

// // Tests the scenario where the resolver returns an update with multiple
// // addresses and the LB policy successfully connects to first one and moves to
// // READY. The resolver then returns an update with a an address list that is
// // identical to the first update. The LB policy should create subchannels for
// // the new addresses, and then see that the currently connected subchannel is in
// // the new address list. It should then send a new READY picker that returns the
// // currently connected subchannel.
// #[tokio::test]
// async fn roundrobin_resolver_update_contains_identical_address_list() {
//     let (mut rx_events, mut child_manager, mut tcc) = setup();
//     let child_manager = child_manager.as_mut();
//     let tcc = tcc.as_mut();

//     let endpoints = create_endpoint_with_n_addresses(2);
//     send_resolver_update_to_policy(child_manager, vec![endpoints.clone()], tcc);
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
    
//     send_initial_subchannel_updates_to_policy(child_manager, &subchannels, tcc);
    
//     verify_connection_attempt_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     move_subchannel_to_connecting(child_manager, subchannels[0].clone(), tcc);
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     verify_connecting_picker_from_policy(&mut rx_events).await;
//     move_subchannel_to_ready(child_manager, subchannels[0].clone(), tcc);
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;

//     send_resolver_update_to_policy(child_manager, vec![endpoints.clone()], tcc);
//     let subchannels =
//         verify_subchannel_creation_from_policy(&mut rx_events, endpoints.addresses.clone()).await;
//     child_manager.subchannel_update(
//         subchannels[0].clone(),
//         &SubchannelState {
//             connectivity_state: ConnectivityState::Ready,
//             ..Default::default()
//         },
//         tcc,
//     );
//     verify_ready_picker_from_policy(&mut rx_events, subchannels[0].clone()).await;
// }



