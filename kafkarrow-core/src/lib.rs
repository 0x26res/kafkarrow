//! # kafkarrow-core
//!
//! Core implementation of kafkarrow: a Kafka consumer that replays topics in
//! timestamp order and returns messages as Apache Arrow record batches.
//!
//! This crate exposes arrow-rs and rdkafka types in its public API, so you
//! must use the same arrow major version (and a compatible rdkafka). Both are
//! re-exported as [`arrow`] and [`rdkafka`] to make that easy. For an API that
//! works with any arrow version, use the `kafkarrow` crate instead.
//!
//! ## Example
//!
//! ```no_run
//! use std::collections::HashMap;
//! use std::time::Duration;
//!
//! use kafkarrow_core::{RealConsumerManager, SourceTopic};
//!
//! let config = HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())]);
//! let mut manager =
//!     RealConsumerManager::create(config, vec![SourceTopic::from_earliest("orders")], None)?;
//! while !manager.is_live() {
//!     let batch = manager.poll_record_batch(Duration::from_millis(100))?;
//!     println!("{} messages", batch.num_rows());
//! }
//! # Ok::<(), String>(())
//! ```

pub mod consumer_manager;
pub mod consumer_trait;
pub mod mock_consumer;
pub mod record_batch;
pub mod source_topic;

#[cfg(test)]
mod test_runner;
#[cfg(test)]
mod test_scenario;

pub use arrow;
pub use rdkafka;

pub use consumer_manager::{
    ConsumerManager, ConsumerMetrics, RealConsumerManager, TimestampedMessage,
};
pub use record_batch::{
    message_schema, messages_to_record_batch, partition_state_schema,
    partition_state_to_record_batch,
};
pub use source_topic::{OffsetPolicy, SourceTopic};
