use std::collections::HashMap;
use std::time::Duration;

use arrow::pyarrow::ToPyArrow;
use kafkarrow_core::{
    message_schema, partition_state_schema, partition_state_to_record_batch, RealConsumerManager,
    SourceTopic,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

struct PySourceTopic(SourceTopic);

impl<'a, 'py> FromPyObject<'a, 'py> for PySourceTopic {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let (name, policy, time_ms): (String, String, Option<i64>) = if let Ok(dict) =
            obj.cast::<PyDict>()
        {
            let name: String = dict
                .get_item("name")?
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing 'name' key"))?
                .extract()?;
            let policy: String = dict
                .get_item("policy")?
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing 'policy' key")
                })?
                .extract()?;
            let time_ms: Option<i64> =
                dict.get_item("time_ms")?.map(|v| v.extract()).transpose()?;
            (name, policy, time_ms)
        } else {
            let name: String = obj.getattr("name")?.extract()?;
            let policy: String = obj.getattr("policy")?.extract()?;
            let time_ms: Option<i64> = obj.getattr("time_ms")?.extract()?;
            (name, policy, time_ms)
        };

        let source_topic = match policy.as_str() {
            "latest" => SourceTopic::from_latest(name),
            "earliest" => SourceTopic::from_earliest(name),
            "relative_time" => {
                let ms = time_ms.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "relative_time policy requires 'time_ms'",
                    )
                })?;
                SourceTopic::from_relative_time(name, ms)
            }
            "absolute_time" => {
                let ms = time_ms.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "absolute_time policy requires 'time_ms'",
                    )
                })?;
                SourceTopic::from_absolute_time(name, ms)
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "unknown policy: '{}'. Expected: latest, earliest, relative_time, absolute_time",
                policy
            )))
            }
        };

        Ok(PySourceTopic(source_topic))
    }
}

#[pyfunction]
fn validate_source_topic(topic: PySourceTopic) -> PyResult<()> {
    let _ = topic.0;
    Ok(())
}

#[pyclass]
struct PyConsumerManager {
    manager: RealConsumerManager,
}

fn runtime_error(e: impl ToString) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
}

#[pymethods]
impl PyConsumerManager {
    #[new]
    #[pyo3(signature = (config, topics, batch_size=None))]
    fn new(
        config: HashMap<String, String>,
        topics: Vec<PySourceTopic>,
        batch_size: Option<usize>,
    ) -> PyResult<Self> {
        let source_topics: Vec<SourceTopic> = topics.into_iter().map(|t| t.0).collect();

        let manager = RealConsumerManager::create(config, source_topics, batch_size)
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(Self { manager })
    }

    fn poll<'py>(&mut self, py: Python<'py>, timeout_ms: u64) -> PyResult<Bound<'py, PyAny>> {
        let batch = py
            .detach(|| {
                self.manager
                    .poll_record_batch(Duration::from_millis(timeout_ms))
            })
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        batch.to_pyarrow(py).map_err(runtime_error)
    }

    fn is_live(&self) -> bool {
        self.manager.is_live()
    }

    fn get_priming_watermark(&self) -> Option<i64> {
        self.manager.get_priming_watermark()
    }

    fn held_message_count(&self) -> usize {
        self.manager.held_message_count()
    }

    fn paused_partition_count(&self) -> usize {
        self.manager.paused_partition_count()
    }

    fn partition_state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        partition_state_to_record_batch(&self.manager)
            .map_err(runtime_error)?
            .to_pyarrow(py)
            .map_err(runtime_error)
    }
}

#[pyfunction]
fn get_message_schema(py: Python<'_>) -> PyResult<Py<PyAny>> {
    message_schema()
        .as_ref()
        .to_pyarrow(py)
        .map(|bound| bound.unbind())
        .map_err(runtime_error)
}

#[pyfunction]
fn get_partition_state_schema(py: Python<'_>) -> PyResult<Py<PyAny>> {
    partition_state_schema()
        .as_ref()
        .to_pyarrow(py)
        .map(|bound| bound.unbind())
        .map_err(runtime_error)
}

/// Version from Cargo.toml, set at compile time.
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add_class::<PyConsumerManager>()?;
    m.add_function(wrap_pyfunction!(validate_source_topic, m)?)?;
    m.add_function(wrap_pyfunction!(get_message_schema, m)?)?;
    m.add_function(wrap_pyfunction!(get_partition_state_schema, m)?)?;
    Ok(())
}
