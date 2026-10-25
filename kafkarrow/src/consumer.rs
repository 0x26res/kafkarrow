use std::collections::HashMap;
use std::time::Duration;

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::datatypes::SchemaRef;
use arrow::ffi::{to_ffi, FFI_ArrowSchema};
use kafkarrow_core::{RealConsumerManager, SourceTopic};

use crate::error::KafkarrowError;
use crate::ffi::{export_array, export_schema, ArrowArray, ArrowSchema};

/// Kafka consumer that replays topics in timestamp order and returns messages
/// as Arrow record batches through the C Data Interface.
pub struct ConsumerManager {
    inner: RealConsumerManager,
}

impl ConsumerManager {
    /// Connect to Kafka and resolve the start and end offsets of every
    /// partition of `topics`.
    ///
    /// `config` takes librdkafka consumer properties (`bootstrap.servers`,
    /// ...). `group.id` defaults to a random id; offsets are never committed.
    /// `batch_size` defaults to 10,000 messages.
    pub fn new(
        config: HashMap<String, String>,
        topics: Vec<SourceTopic>,
        batch_size: Option<usize>,
    ) -> Result<Self, KafkarrowError> {
        let inner = RealConsumerManager::create(config, topics, batch_size)
            .map_err(KafkarrowError::Kafka)?;
        Ok(Self { inner })
    }

    /// Poll for messages and return the ones that can be released in
    /// timestamp order, as a struct array with the [`message_schema`] fields.
    pub fn poll(&mut self, timeout: Duration) -> Result<(ArrowArray, ArrowSchema), KafkarrowError> {
        let batch = self
            .inner
            .poll_record_batch(timeout)
            .map_err(KafkarrowError::Kafka)?;
        export_record_batch(batch)
    }

    /// Snapshot of every assigned partition's state, as a struct array with
    /// the [`partition_state_schema`] fields.
    pub fn partition_state(&self) -> Result<(ArrowArray, ArrowSchema), KafkarrowError> {
        let batch = kafkarrow_core::partition_state_to_record_batch(&self.inner)
            .map_err(|e| KafkarrowError::Arrow(e.to_string()))?;
        export_record_batch(batch)
    }

    /// Whether every partition has caught up with its end offset.
    pub fn is_live(&self) -> bool {
        self.inner.is_live()
    }

    /// Timestamp (ms since epoch) up to which messages have been released.
    pub fn priming_watermark(&self) -> Option<i64> {
        self.inner.get_priming_watermark()
    }

    /// Number of consumed messages held back until they can be released in order.
    pub fn held_message_count(&self) -> usize {
        self.inner.held_message_count()
    }

    /// Number of partitions currently paused for backpressure.
    pub fn paused_partition_count(&self) -> usize {
        self.inner.paused_partition_count()
    }
}

/// Struct schema of the arrays returned by [`ConsumerManager::poll`].
pub fn message_schema() -> Result<ArrowSchema, KafkarrowError> {
    export_struct_schema(kafkarrow_core::message_schema())
}

/// Struct schema of the arrays returned by [`ConsumerManager::partition_state`].
pub fn partition_state_schema() -> Result<ArrowSchema, KafkarrowError> {
    export_struct_schema(kafkarrow_core::partition_state_schema())
}

fn export_struct_schema(schema: SchemaRef) -> Result<ArrowSchema, KafkarrowError> {
    let schema = FFI_ArrowSchema::try_from(schema.as_ref())
        .map_err(|e| KafkarrowError::Arrow(e.to_string()))?;
    Ok(export_schema(schema))
}

fn export_record_batch(batch: RecordBatch) -> Result<(ArrowArray, ArrowSchema), KafkarrowError> {
    let struct_array = StructArray::from(batch);
    let (array, schema) =
        to_ffi(&struct_array.to_data()).map_err(|e| KafkarrowError::Arrow(e.to_string()))?;
    Ok((export_array(array), export_schema(schema)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Int64Type, Schema};
    use arrow::ffi::{from_ffi, FFI_ArrowArray};
    use kafkarrow_core::{messages_to_record_batch, TimestampedMessage};

    fn import(array: ArrowArray, schema: ArrowSchema) -> RecordBatch {
        let array: FFI_ArrowArray = unsafe { std::mem::transmute(array) };
        let schema: FFI_ArrowSchema = unsafe { std::mem::transmute(schema) };
        let data = unsafe { from_ffi(array, &schema) }.unwrap();
        StructArray::from(data).into()
    }

    #[test]
    fn test_export_record_batch_roundtrip() {
        let messages = vec![TimestampedMessage {
            key: None,
            value: Some(b"v".to_vec()),
            topic: "orders".to_string(),
            partition: 0,
            offset: 42,
            timestamp_ms: 1_000,
        }];
        let batch = messages_to_record_batch(&messages).unwrap();
        let (array, schema) = export_record_batch(batch.clone()).unwrap();
        let imported = import(array, schema);
        assert_eq!(imported, batch);
        assert_eq!(imported.column(4).as_primitive::<Int64Type>().value(0), 42);
    }

    #[test]
    fn test_message_schema() {
        let schema: FFI_ArrowSchema = unsafe { std::mem::transmute(message_schema().unwrap()) };
        match DataType::try_from(&schema).unwrap() {
            DataType::Struct(fields) => {
                assert_eq!(Schema::new(fields), *kafkarrow_core::message_schema())
            }
            other => panic!("expected a struct, got {other}"),
        }
    }
}
