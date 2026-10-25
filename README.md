# kafkarrow

> [!NOTE]
> kafkarrow was previously published as `kafkars`. It was renamed to avoid a clash with the newly created [`kafkars`](https://crates.io/crates/kafkars) Rust crate. Only the package name changed: `pip install kafkarrow` and `import kafkarrow`.

[![PyPI Version][pypi-image]][pypi-url]
[![Python Version][versions-image]][versions-url]
[![PyPI Wheel][wheel-image]][wheel-url]
[![Documentation][docs-image]][docs-url]
[![Downloads][downloads-image]][downloads-url]
[![Downloads][downloads-month-image]][downloads-month-url]
[![Build Status][build-image]][build-url]
[![Tests][test-image]][test-url]
[![codecov][codecov-image]][codecov-url]
[![License][license-image]][license-url]
[![Ruff][ruff-image]][ruff-url]
[![Github Stars][stars-image]][stars-url]
[![GitHub Forks][forks-image]][forks-url]
[![GitHub issues][github-issues-image]][github-issues-url]
[![GitHub Release][release-image]][release-url]
[![Release Date][release-date-image]][release-url]
[![Last Commit][last-commit-image]][commits-url]
[![Commit Activity][commit-activity-image]][commits-url]
[![Open PRs][open-prs-image]][prs-url]
[![Contributors][contributors-image]][contributors-url]
[![Contributing][contributing-image]][contributing-url]
[![Repo Size][repo-size-image]][repo-size-url]
[![Rust][rust-image]][rust-url]
[![Apache Arrow][apache-arrow-image]][apache-arrow-url]
[![prek][prek-image]][prek-url]

[Repository](https://github.com/0x26res/kafkarrow) |
[Documentation](https://kafkarrow.readthedocs.io/) |
[PyPI](https://pypi.org/project/kafkarrow/) |
[Issues](https://github.com/0x26res/kafkarrow/issues) |
[Releases](https://github.com/0x26res/kafkarrow/releases)

Rust-based, Arrow-powered Python Kafka client for high-throughput data pipelines.

## Motivation

Python's Global Interpreter Lock (GIL) and memory management create bottlenecks when consuming high-volume Kafka streams.
Traditional Python Kafka clients process messages one at a time,
requiring serialization/deserialization overhead for each message and limiting throughput.

**kafkarrow** solves this by:

- **Rust core**: All Kafka operations (polling, buffering, ordering) happen in Rust, bypassing the GIL
- **Batch processing**: Messages are accumulated and returned as Apache Arrow RecordBatches, not individual Python objects
- **Zero-copy where possible**: Arrow's columnar format enables efficient data transfer between Rust and Python
- **Vectorized operations**: Process thousands of messages at once with pandas, polars, or any Arrow-compatible library

This architecture is ideal for:

- Real-time analytics pipelines
- ML feature stores consuming from Kafka
- High-volume event processing
- Data lake ingestion

## Important: Analytics-Focused Design

**kafkarrow does not commit offsets.** It is designed for analytics and high-throughput batch processing, not transactional workloads.

- **No exactly-once semantics**: Messages may be reprocessed if your application restarts
- **No offset tracking**: You control where to start reading via offset policies
- **Stateless consumers**: Each consumer instance starts fresh based on the configured policy

If you need exactly-once processing, transactional guarantees, or automatic offset management, use a traditional Kafka client like `confluent-kafka-python`.

## Features

- **Ordered delivery**: Messages released in timestamp order across all partitions
- **Flexible offset policies**: Start from earliest, latest, or any timestamp
- **Backpressure management**: Automatically pauses fast partitions to prevent memory overflow
- **Arrow-native output**: Returns PyArrow RecordBatch for efficient downstream processing

## Installation

```bash
pip install kafkarrow
```

## Quick Start

```python
from kafkarrow import ConsumerManager, SourceTopic

# Define source topics with offset policies
topics = [
    SourceTopic.from_earliest("events"),
    SourceTopic.from_relative_time("metrics", 3600_000),  # 1 hour ago
]

# Create consumer
manager = ConsumerManager(
    config={"bootstrap.servers": "localhost:9092"},
    topics=topics,
)

# Poll returns PyArrow RecordBatch
while True:
    batch = manager.poll(timeout_ms=1000)
    if batch.num_rows > 0:
        # Convert to pandas/polars for processing
        df = batch.to_pandas()
        print(f"Received {len(df)} messages")

    if manager.is_live():
        print("Caught up to real-time")
        break
```

## Message Schema

Each poll returns a RecordBatch with the following schema:

| Column      | Type                 | Description                |
|-------------|----------------------|----------------------------|
| `key`       | `binary`             | Message key (nullable)     |
| `value`     | `binary`             | Message payload (nullable) |
| `topic`     | `utf8`               | Source topic name          |
| `partition` | `int32`              | Partition number           |
| `offset`    | `int64`              | Message offset             |
| `timestamp` | `timestamp[ms, UTC]` | Message timestamp          |

## Offset Policies

| Policy                          | Description                            |
|---------------------------------|----------------------------------------|
| `from_earliest(topic)`          | Start from the beginning               |
| `from_latest(topic)`            | Start from the end (new messages only) |
| `from_relative_time(topic, ms)` | Start from N milliseconds ago          |
| `from_absolute_time(topic, ms)` | Start from specific Unix timestamp     |

## Rust

kafkarrow is also available as two Rust crates:

- [`kafkarrow`](https://crates.io/crates/kafkarrow): returns record batches through the Arrow C Data Interface, so it works with any arrow-rs version. Its public API has no arrow or rdkafka types.
- [`kafkarrow-core`](https://crates.io/crates/kafkarrow-core): the implementation, with an API that uses arrow-rs and rdkafka types directly. You need to use the same arrow major version.

See [`kafkarrow/README.md`](kafkarrow/README.md) for usage.

## Documentation

Full documentation is available at [kafkarrow.readthedocs.io](https://kafkarrow.readthedocs.io).

## License

Apache 2.0

[pypi-image]: https://img.shields.io/pypi/v/kafkarrow
[pypi-url]: https://pypi.org/project/kafkarrow/
[versions-image]: https://img.shields.io/pypi/pyversions/kafkarrow
[versions-url]: https://pypi.org/project/kafkarrow/
[wheel-image]: https://img.shields.io/pypi/wheel/kafkarrow
[wheel-url]: https://pypi.org/project/kafkarrow/
[docs-image]: https://readthedocs.org/projects/kafkarrow/badge/?version=latest
[docs-url]: https://kafkarrow.readthedocs.io/en/latest/
[downloads-image]: https://static.pepy.tech/badge/kafkarrow
[downloads-url]: https://pepy.tech/project/kafkarrow
[downloads-month-image]: https://static.pepy.tech/badge/kafkarrow/month
[downloads-month-url]: https://pepy.tech/project/kafkarrow
[build-image]: https://github.com/0x26res/kafkarrow/actions/workflows/ci.yaml/badge.svg
[build-url]: https://github.com/0x26res/kafkarrow/actions/workflows/ci.yaml
[test-image]: https://github.com/0x26res/kafkarrow/actions/workflows/test.yaml/badge.svg
[test-url]: https://github.com/0x26res/kafkarrow/actions/workflows/test.yaml
[codecov-image]: https://codecov.io/gh/0x26res/kafkarrow/branch/main/graph/badge.svg
[codecov-url]: https://codecov.io/gh/0x26res/kafkarrow
[license-image]: https://img.shields.io/pypi/l/kafkarrow
[license-url]: https://github.com/0x26res/kafkarrow/blob/main/LICENSE
[ruff-image]: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
[ruff-url]: https://github.com/astral-sh/ruff
[stars-image]: https://img.shields.io/github/stars/0x26res/kafkarrow
[stars-url]: https://github.com/0x26res/kafkarrow
[forks-image]: https://img.shields.io/github/forks/0x26res/kafkarrow
[forks-url]: https://github.com/0x26res/kafkarrow/network/members
[github-issues-image]: https://img.shields.io/github/issues/0x26res/kafkarrow
[github-issues-url]: https://github.com/0x26res/kafkarrow/issues
[contributing-image]: https://img.shields.io/badge/PR-Welcome-%23FF8300.svg
[contributing-url]: https://github.com/0x26res/kafkarrow/pulls
[repo-size-image]: https://img.shields.io/github/repo-size/0x26res/kafkarrow
[repo-size-url]: https://github.com/0x26res/kafkarrow
[last-commit-image]: https://img.shields.io/github/last-commit/0x26res/kafkarrow
[commits-url]: https://github.com/0x26res/kafkarrow/commits/main
[commit-activity-image]: https://img.shields.io/github/commit-activity/m/0x26res/kafkarrow
[open-prs-image]: https://img.shields.io/github/issues-pr/0x26res/kafkarrow
[prs-url]: https://github.com/0x26res/kafkarrow/pulls
[contributors-image]: https://img.shields.io/github/contributors/0x26res/kafkarrow
[contributors-url]: https://github.com/0x26res/kafkarrow/graphs/contributors
[release-image]: https://img.shields.io/github/v/release/0x26res/kafkarrow
[release-date-image]: https://img.shields.io/github/release-date/0x26res/kafkarrow
[release-url]: https://github.com/0x26res/kafkarrow/releases
[rust-image]: https://img.shields.io/badge/rust-%23000000.svg?logo=rust&logoColor=white
[rust-url]: https://www.rust-lang.org/
[apache-arrow-image]: https://img.shields.io/badge/Apache%20Arrow-powered-orange.svg
[apache-arrow-url]: https://arrow.apache.org/
[prek-image]: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/j178/prek/master/docs/assets/badge-v0.json
[prek-url]: https://github.com/j178/prek
