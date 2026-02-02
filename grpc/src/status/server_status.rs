use super::StatusError;

/// Represents a gRPC status on the server.
///
/// This is a separate type from `Status` to prevent accidental conversion and
/// leaking of sensitive information from the server to the client.
#[derive(Debug, Clone)]
pub struct ServerStatusError(StatusError);

pub type ServerStatus = Result<(), ServerStatusError>;

impl std::ops::Deref for ServerStatusError {
    type Target = StatusError;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ServerStatusError {
    /// Create a new `ServerStatus` from a `StatusError`.
    pub fn from_status_error(status_error: StatusError) -> Self {
        ServerStatusError(status_error)
    }

    /// Converts the `ServerStatusError` to a `StatusError` for client responses.
    pub(crate) fn into_status_error(self) -> StatusError {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::StatusCode;

    use super::*;

    #[test]
    fn test_server_status_error_deref() {
        let status =
            ServerStatusError::from_status_error(StatusError::new(StatusCode::Internal, "not ok"));
        assert_eq!(status.code(), StatusCode::Internal);
    }

    #[test]
    fn test_server_status_error_from_status_error() {
        let status_error = StatusError::new(StatusCode::Internal, "not ok");
        let server_status = ServerStatusError::from_status_error(status_error);
        assert_eq!(server_status.code(), StatusCode::Internal);
    }

    #[test]
    fn test_server_status_error_into_status_error() {
        let server_status =
            ServerStatusError::from_status_error(StatusError::new(StatusCode::Internal, "not ok"));
        let status_error = server_status.into_status_error();
        assert_eq!(status_error.code(), StatusCode::Internal);
    }
}
