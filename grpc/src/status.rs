mod server_status;
mod status_code;

pub use server_status::ServerStatus;
pub use status_code::StatusCode;

/// Represents a gRPC status.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusError {
    code: StatusCode,
    message: String,
}

pub type Status = Result<(), StatusError>;

impl StatusError {
    /// Create a new `Status` with the given code and message.
    pub fn new(code: StatusCode, message: impl Into<String>) -> Self {
        StatusError {
            code,
            message: message.into(),
        }
    }

    /// Get the `StatusCode` of this `StatusError`.
    pub fn code(&self) -> StatusCode {
        self.code
    }

    /// Get the message of this `Status`.
    pub fn message(&self) -> &str {
        &self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_new() {
        let status = StatusError::new(StatusCode::Unknown, "not ok");
        assert_eq!(status.code(), StatusCode::Unknown);
        assert_eq!(status.message(), "not ok");
    }

    #[test]
    fn test_status_debug() {
        let status = StatusError::new(StatusCode::Internal, "not ok");
        let debug = format!("{:?}", status);
        assert!(debug.contains("Status"));
        assert!(debug.contains("Internal"));
        assert!(debug.contains("not ok"));
    }
}
