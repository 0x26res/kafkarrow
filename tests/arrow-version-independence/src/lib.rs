//! Proves that kafkarrow can hand Arrow data to a consumer compiled against a
//! different arrow major version, via the Arrow C Data Interface.
//!
//! This crate pins arrow 58 while kafkarrow internally uses a newer major
//! (see the two Cargo.toml files). If kafkarrow ever leaked arrow types in its
//! public API, or broke the C Data Interface contract, these tests would fail
//! to compile or fail at runtime.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::{Array, StructArray};
    use arrow::datatypes::{DataType, Schema, TimeUnit};
    use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use kafkarrow::ConsumerManager;

    // Both the consumer's arrow and kafkarrow::ffi implement the same frozen
    // C ABI (checked by compile-time layout assertions inside kafkarrow), so
    // the structs can be transmuted into one another.
    fn import_schema(schema: kafkarrow::ffi::ArrowSchema) -> Schema {
        let schema =
            unsafe { std::mem::transmute::<kafkarrow::ffi::ArrowSchema, FFI_ArrowSchema>(schema) };
        match DataType::try_from(&schema).unwrap() {
            DataType::Struct(fields) => Schema::new(fields),
            other => panic!("expected a struct, got {other}"),
        }
    }

    fn import_batch(
        array: kafkarrow::ffi::ArrowArray,
        schema: kafkarrow::ffi::ArrowSchema,
    ) -> RecordBatch {
        let array =
            unsafe { std::mem::transmute::<kafkarrow::ffi::ArrowArray, FFI_ArrowArray>(array) };
        let schema =
            unsafe { std::mem::transmute::<kafkarrow::ffi::ArrowSchema, FFI_ArrowSchema>(schema) };
        let data = unsafe { from_ffi(array, &schema) }.unwrap();
        StructArray::from(data).into()
    }

    #[test]
    fn message_schema_imports_with_older_arrow() {
        let schema = import_schema(kafkarrow::message_schema().unwrap());
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            ["key", "value", "topic", "partition", "offset", "timestamp"]
        );
        assert_eq!(
            schema.field_with_name("timestamp").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
    }

    #[test]
    fn partition_state_imports_with_older_arrow() {
        // No topics: the consumer is created without contacting a broker.
        let config = HashMap::from([("bootstrap.servers".to_string(), "localhost:1".to_string())]);
        let manager = ConsumerManager::new(config, vec![], None).unwrap();

        let (array, schema) = manager.partition_state().unwrap();
        let batch = import_batch(array, schema);

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(
            batch.schema().as_ref(),
            &import_schema(kafkarrow::partition_state_schema().unwrap())
        );
        assert!(batch.column(0).is_empty());
    }
}
