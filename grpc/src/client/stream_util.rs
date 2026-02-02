use crate::client::RecvStream;
use crate::core::ClientResponseStreamItem;
use crate::core::RecvMessage;
use crate::core::ResponseStreamItem;
use crate::core::Trailers;
use crate::status::StatusError;
use crate::StatusCode;

enum RecvStreamState {
    AwaitingHeaders,
    AwaitingMessagesOrTrailers,
    AwaitingTrailers,
    Done,
}

/// RecvStreamValidator wraps a client's RecvStream and enforces proper
/// RecvStream semantics on it so that protocol validation does not need to be
/// handled by the consumer.
pub struct RecvStreamValidator<R> {
    recv_stream: R,
    state: RecvStreamState,
    unary_response: bool,
}

impl<R> RecvStreamValidator<R>
where
    R: RecvStream,
{
    /// Constructs a new `RecvStreamValidator` for converting an untrusted
    /// `RecvStream` into one that enforces the proper gRPC response stream
    /// protocol.  If the protocol is violated an error will be synthesized.
    /// Any calls to the `RecvStream` impl's `next` method beyond `Trailers`
    /// will not be propagated and will immediately return `StreamClosed`.
    pub fn new(recv_stream: R, unary_response: bool) -> Self {
        Self {
            recv_stream,
            state: RecvStreamState::AwaitingHeaders,
            unary_response,
        }
    }

    /// Sets the state to Done and produces a synthesized trailer item
    /// containing the error message.
    fn error(&mut self, s: impl Into<String>) -> ClientResponseStreamItem {
        self.state = RecvStreamState::Done;
        ResponseStreamItem::Trailers(Trailers {
            status: Err(StatusError::new(StatusCode::Internal, s)),
        })
    }
}

impl<R> RecvStream for RecvStreamValidator<R>
where
    R: RecvStream,
{
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientResponseStreamItem {
        // Never call the underlying RecvStream if done.
        if matches!(self.state, RecvStreamState::Done) {
            return ResponseStreamItem::StreamClosed;
        }

        let item = self.recv_stream.next(msg).await;

        match item {
            ResponseStreamItem::Headers(_) => {
                if matches!(self.state, RecvStreamState::AwaitingHeaders) {
                    self.state = RecvStreamState::AwaitingMessagesOrTrailers;
                    item
                } else {
                    self.error("stream received multiple headers")
                }
            }
            ResponseStreamItem::Message(_) => {
                if matches!(self.state, RecvStreamState::AwaitingMessagesOrTrailers) {
                    if self.unary_response {
                        self.state = RecvStreamState::AwaitingTrailers;
                    }
                    item
                } else if matches!(self.state, RecvStreamState::AwaitingTrailers) {
                    self.error("unary stream received multiple messages")
                } else {
                    self.error("stream received messages without headers")
                }
            }
            ResponseStreamItem::Trailers(t) => {
                if self.unary_response
                    && !matches!(self.state, RecvStreamState::AwaitingTrailers)
                    && t.status.is_ok()
                {
                    return self.error("unary stream received zero messages");
                }
                // Always return a trailers result immediately - it is valid any
                // time but sets the stream's state to Done.
                self.state = RecvStreamState::Done;
                ResponseStreamItem::Trailers(t)
            }
            ResponseStreamItem::StreamClosed => {
                // Trailers were never received or we would be Done.
                self.error("stream ended without trailers")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::mem::discriminant;
    use std::vec;

    use tokio::sync::mpsc::Receiver;
    use tokio::sync::mpsc::Sender;

    use super::*;
    use crate::client::RecvStream;
    use crate::core::ClientResponseStreamItem;
    use crate::core::RecvMessage;
    use crate::core::ResponseHeaders;
    use crate::core::Trailers;

    // Tests that an error occurs if messages are received before headers.
    #[tokio::test]
    async fn test_validator_messages_before_headers() {
        let scenarios = [vec![ResponseStreamItem::Message(())]];

        for scenario in scenarios {
            validate_scenario(
                &scenario,
                ResponseStreamItem::Trailers(Trailers {
                    status: Err(StatusError::new(
                        StatusCode::Internal,
                        "received messages without headers",
                    )),
                }),
                false,
            )
            .await;
        }
    }

    // Tests that an error occurs if StreamClosed is received early.
    #[tokio::test]
    async fn test_validator_stream_closed_before_trailers() {
        let scenarios = [
            vec![ResponseStreamItem::StreamClosed],
            vec![
                ResponseStreamItem::Headers(ResponseHeaders {}),
                ResponseStreamItem::StreamClosed,
            ],
            vec![
                ResponseStreamItem::Headers(ResponseHeaders {}),
                ResponseStreamItem::Message(()),
                ResponseStreamItem::StreamClosed,
            ],
        ];

        for scenario in &scenarios {
            validate_scenario(
                scenario,
                ResponseStreamItem::Trailers(Trailers {
                    status: Err(StatusError::new(
                        StatusCode::Internal,
                        "ended without trailers",
                    )),
                }),
                false,
            )
            .await;
        }
    }

    // Tests that an error occurs if headers are received twice.
    #[tokio::test]
    async fn test_validator_headers_repeated() {
        let scenarios = [
            vec![
                ResponseStreamItem::Headers(ResponseHeaders {}),
                ResponseStreamItem::Headers(ResponseHeaders {}),
            ],
            vec![
                ResponseStreamItem::Headers(ResponseHeaders {}),
                ResponseStreamItem::Message(()),
                ResponseStreamItem::Headers(ResponseHeaders {}),
            ],
        ];

        for scenario in &scenarios {
            validate_scenario(
                scenario,
                ResponseStreamItem::Trailers(Trailers {
                    status: Err(StatusError::new(
                        StatusCode::Internal,
                        "received multiple headers",
                    )),
                }),
                false,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_validator_unary_ok_without_message() {
        let scenarios = [
            vec![ResponseStreamItem::Trailers(Trailers { status: Ok(()) })],
            vec![
                ResponseStreamItem::Headers(ResponseHeaders {}),
                ResponseStreamItem::Trailers(Trailers { status: Ok(()) }),
            ],
        ];

        for scenario in &scenarios {
            validate_scenario(
                scenario,
                ResponseStreamItem::Trailers(Trailers {
                    status: Err(StatusError::new(
                        StatusCode::Internal,
                        "received zero messages",
                    )),
                }),
                true,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_validator_unary_multiple_messages() {
        let scenarios = [vec![
            ResponseStreamItem::Headers(ResponseHeaders {}),
            ResponseStreamItem::Message(()),
            ResponseStreamItem::Message(()),
        ]];

        for scenario in &scenarios {
            validate_scenario(
                scenario,
                ResponseStreamItem::Trailers(Trailers {
                    status: Err(StatusError::new(
                        StatusCode::Internal,
                        "received multiple messages",
                    )),
                }),
                true,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_validator_successful_stream() {
        let scenarios = [vec![
            ResponseStreamItem::Headers(ResponseHeaders {}),
            ResponseStreamItem::Message(()),
            ResponseStreamItem::Message(()),
            ResponseStreamItem::Message(()),
            ResponseStreamItem::Trailers(Trailers { status: Ok(()) }),
        ]];

        for scenario in &scenarios {
            validate_scenario(
                scenario,
                ResponseStreamItem::Trailers(Trailers { status: Ok(()) }),
                false,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_validator_erroring_stream() {
        let scenarios = [vec![
            ResponseStreamItem::Headers(ResponseHeaders {}),
            ResponseStreamItem::Message(()),
            ResponseStreamItem::Message(()),
            ResponseStreamItem::Message(()),
            ResponseStreamItem::Trailers(Trailers {
                status: Err(StatusError::new(StatusCode::Aborted, "some err")),
            }),
        ]];

        for scenario in &scenarios {
            validate_scenario(
                scenario,
                ResponseStreamItem::Trailers(Trailers {
                    status: Err(StatusError::new(StatusCode::Aborted, "some err")),
                }),
                false,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_validator_successful_unary() {
        let scenarios = [vec![
            ResponseStreamItem::Headers(ResponseHeaders {}),
            ResponseStreamItem::Message(()),
            ResponseStreamItem::Trailers(Trailers { status: Ok(()) }),
        ]];

        for scenario in &scenarios {
            validate_scenario(
                scenario,
                ResponseStreamItem::Trailers(Trailers { status: Ok(()) }),
                true,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_validator_erroring_unary() {
        let scenarios = [
            vec![ResponseStreamItem::Trailers(Trailers {
                status: Err(StatusError::new(StatusCode::Aborted, "some err")),
            })],
            vec![
                ResponseStreamItem::Headers(ResponseHeaders {}),
                ResponseStreamItem::Trailers(Trailers {
                    status: Err(StatusError::new(StatusCode::Aborted, "some err")),
                }),
            ],
            vec![
                ResponseStreamItem::Headers(ResponseHeaders {}),
                ResponseStreamItem::Message(()),
                ResponseStreamItem::Trailers(Trailers {
                    status: Err(StatusError::new(StatusCode::Aborted, "some err")),
                }),
            ],
        ];

        for scenario in &scenarios {
            validate_scenario(
                scenario,
                ResponseStreamItem::Trailers(Trailers {
                    status: Err(StatusError::new(StatusCode::Aborted, "some err")),
                }),
                true,
            )
            .await;
        }
    }

    async fn validate_scenario(
        scenario: &[ResponseStreamItem<()>],
        expect: ResponseStreamItem<()>,
        unary: bool,
    ) {
        let (mock_stream, tx) = MockRecvStream::new();
        let mut validator = RecvStreamValidator::new(mock_stream, unary);
        // Send all but the last item, verifying it is returned by the
        // validator.
        for item in &scenario[..scenario.len() - 1] {
            tx.send(item.clone()).await.unwrap();
            let got = validator.next(&mut NopRecvMessage).await;
            // Assert that the item sent is the same type as the item received.
            println!("{got:?} vs {item:?}");
            assert_eq!(discriminant(&got), discriminant(item));
        }
        // Send the final item.
        tx.send(scenario[scenario.len() - 1].clone()).await.unwrap();
        let got = validator.next(&mut NopRecvMessage).await;
        assert!(matches!(&got, expect));
        if let ResponseStreamItem::Trailers(got_t) = got {
            let ResponseStreamItem::Trailers(expect_t) = expect else {
                unreachable!(); // per matches check above
            };
            // Assert the codes match.
            assert_eq!(
                got_t.status.as_ref().map_err(|e| e.code()),
                expect_t.status.as_ref().map_err(|e| e.code())
            );
            // Assert the status received contains the expected status error message.
            if let Err(got_t_err) = got_t.status {
                assert!(got_t_err
                    .message()
                    .contains(expect_t.status.unwrap_err().message()));
            }
        }
    }

    pub struct NopRecvMessage;

    impl RecvMessage for NopRecvMessage {
        fn decode(&mut self, data: Vec<Vec<u8>>) {}
    }

    pub struct MockRecvStream {
        rx: Receiver<ClientResponseStreamItem>,
    }

    impl RecvStream for MockRecvStream {
        async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientResponseStreamItem {
            self.rx.recv().await.unwrap()
        }
    }

    impl MockRecvStream {
        fn new() -> (Self, Sender<ClientResponseStreamItem>) {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            (Self { rx }, tx)
        }
    }
}
