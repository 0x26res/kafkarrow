# kafkarrow

[![Crates.io](https://img.shields.io/crates/v/kafkarrow.svg)](https://crates.io/crates/kafkarrow)
[![Documentation](https://docs.rs/kafkarrow/badge.svg)](https://docs.rs/kafkarrow)
[![License](https://img.shields.io/crates/l/kafkarrow.svg)](https://github.com/0x26res/kafkarrow/blob/main/LICENSE)

Kafka consumer that replays topics in timestamp order and returns messages as
Apache Arrow record batches, **without pinning you to an arrow version**.

This crate's public API contains no arrow (or rdkafka) types. Arrow data
crosses the API boundary through the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html),
whose ABI is frozen by the Arrow specification. You can depend on `kafkarrow`
alongside **any** arrow-rs version and receive record batches zero-copy.

If you prefer a conventional arrow-rs-typed API (and are happy to match
kafkarrow's arrow major version), use the
[`kafkarrow-core`](https://crates.io/crates/kafkarrow-core) crate instead. It
contains the actual implementation.

## Usage

```toml
[dependencies]
kafkarrow = "0.0"
arrow = "58"  # any version you like, kafkarrow does not care
```

```rust
use std::collections::HashMap;
use std::time::Duration;

use kafkarrow::{ConsumerManager, SourceTopic};

let config = HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())]);
let mut manager = ConsumerManager::new(config, vec![SourceTopic::from_earliest("orders")], None)?;

// Poll returns the released messages as an Arrow C Data Interface pair...
let (array, schema) = manager.poll(Duration::from_millis(100))?;

// ...which you import with *your* arrow version, zero-copy. kafkarrow's
// structs and arrow's FFI structs implement the same frozen C ABI:
let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { std::mem::transmute(array) };
let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { std::mem::transmute(schema) };
let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema)? };
let batch: arrow::record_batch::RecordBatch = arrow::array::StructArray::from(data).into();
```

## librdkafka

rdkafka is hidden from the API, but `rdkafka-sys` links the native
librdkafka, and Cargo allows only one `rdkafka-sys` version per build. If your
project also uses rdkafka directly, it must resolve to the same `rdkafka-sys`
major version as kafkarrow.

## How the version independence is enforced

- The `kafkarrow::ffi::ArrowArray`/`ArrowSchema` structs are hand-written from
  the Arrow specification; compile-time assertions check they stay
  layout-compatible with the arrow version used internally.
- CI runs an integration test that consumes kafkarrow from a crate pinned to a
  *different* arrow major version (`tests/arrow-version-independence`).
- CI checks the public API (`cargo public-api`) and fails if any `arrow*::` or
  `rdkafka*::` path ever appears in it.

## License

Apache-2.0
