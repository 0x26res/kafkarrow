# Installation

## Requirements

- Python 3.10 or higher
- A running Kafka cluster

## Install from PyPI

```bash
pip install kafkarrow
```

## Install with extras

For development:

```bash
pip install kafkarrow[docs]
```

## Verify Installation

```python
import kafkarrow

print(kafkarrow.__version__)
```

## Dependencies

kafkarrow has minimal dependencies:

- **pyarrow** (>=14): For Arrow-based data transfer

The Rust core is compiled and bundled with the wheel, so no Rust toolchain is required for installation.
