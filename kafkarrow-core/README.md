# kafkarrow-core

[![Crates.io](https://img.shields.io/crates/v/kafkarrow-core.svg)](https://crates.io/crates/kafkarrow-core)
[![Documentation](https://docs.rs/kafkarrow-core/badge.svg)](https://docs.rs/kafkarrow-core)
[![License](https://img.shields.io/crates/l/kafkarrow-core.svg)](https://github.com/0x26res/kafkarrow/blob/main/LICENSE)

Kafka consumer that replays topics in timestamp order and returns messages as
Apache Arrow record batches.

This crate exposes arrow-rs and rdkafka types in its public API, so you must
use the same arrow major version (and a compatible rdkafka). Both are
re-exported as `kafkarrow_core::arrow` and `kafkarrow_core::rdkafka`. For an
API that works with any arrow version, use the
[`kafkarrow`](https://crates.io/crates/kafkarrow) crate instead.

## Usage

```toml
[dependencies]
kafkarrow-core = "0.0"
```

```rust
use std::collections::HashMap;
use std::time::Duration;

use kafkarrow_core::{RealConsumerManager, SourceTopic};

let config = HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())]);
let mut manager =
    RealConsumerManager::create(config, vec![SourceTopic::from_earliest("orders")], None)?;
while !manager.is_live() {
    let batch = manager.poll_record_batch(Duration::from_millis(100))?;
    println!("{} messages", batch.num_rows());
}
```

`ConsumerManager` is generic over the `KafkaConsumer` trait, so it can also be
driven by the `MockConsumer` in tests.

## License

Apache-2.0
