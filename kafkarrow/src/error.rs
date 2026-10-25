use std::fmt;

/// Error type for kafkarrow operations.
///
/// Carries only plain strings so that no arrow or rdkafka types appear in
/// this crate's public API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkarrowError {
    /// Creating the consumer or consuming from Kafka failed.
    Kafka(String),
    /// Arrow data could not be built or exported through the C Data Interface.
    Arrow(String),
}

impl fmt::Display for KafkarrowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkarrowError::Kafka(msg) => write!(f, "kafka error: {}", msg),
            KafkarrowError::Arrow(msg) => write!(f, "arrow error: {}", msg),
        }
    }
}

impl std::error::Error for KafkarrowError {}
