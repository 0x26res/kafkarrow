//! # kafkarrow
//!
//! Kafka consumer that replays topics in timestamp order and returns messages
//! as Apache Arrow record batches.
//!
//! This crate is **arrow-version-independent**: its public API contains no
//! arrow-rs (or rdkafka) types. Arrow data crosses the API boundary through the
//! [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//! ([`ffi::ArrowArray`]/[`ffi::ArrowSchema`]), whose ABI is frozen by the
//! Arrow specification. You can therefore depend on `kafkarrow` together with
//! any arrow version and receive data zero-copy.
//!
//! rdkafka is hidden too, but `rdkafka-sys` links the native librdkafka, and
//! Cargo allows only one `rdkafka-sys` version per build. If you also use
//! rdkafka directly, it must resolve to the same `rdkafka-sys` major version.
//!
//! The implementation lives in the `kafkarrow-core` crate, which exposes a
//! conventional arrow-rs-typed API if you prefer to pin the same arrow version
//! as kafkarrow.
//!
//! ## Example
//!
//! ```ignore
//! use std::collections::HashMap;
//! use std::time::Duration;
//!
//! use kafkarrow::{ConsumerManager, SourceTopic};
//!
//! let config = HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())]);
//! let mut manager = ConsumerManager::new(config, vec![SourceTopic::from_earliest("orders")], None)?;
//!
//! let (array, schema) = manager.poll(Duration::from_millis(100))?;
//!
//! // Import the result with *your* arrow version (any major), zero-copy.
//! // Both struct definitions implement the same frozen C ABI, so they can be
//! // transmuted into one another:
//! let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { std::mem::transmute(array) };
//! let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { std::mem::transmute(schema) };
//! let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema)? };
//! let batch: arrow::record_batch::RecordBatch =
//!     arrow::array::StructArray::from(data).into();
//! ```

pub mod ffi;

mod consumer;
mod error;

pub use consumer::{message_schema, partition_state_schema, ConsumerManager};
pub use error::KafkarrowError;
// Plain configuration types, free of arrow/rdkafka types, shared with kafkarrow-core.
pub use kafkarrow_core::{OffsetPolicy, SourceTopic};
