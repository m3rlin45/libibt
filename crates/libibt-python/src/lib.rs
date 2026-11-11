use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use libibt::{IbtFile, MetadataValue};

/// iRacing IBT telemetry file parser implemented in Rust.
#[pymodule]
fn _libibt_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ibt, m)?)?;
    Ok(())
}

/// Parse an iRacing IBT file and return a LogFile.
///
/// Every variable maps to one channel under its own name: scalars produce
/// plain value columns, time-subsample arrays (`_ST` suffix) are merged
/// into a single higher-rate channel, and other array variables (count >
/// 1, e.g. per-car `CarIdx` arrays) become vector-valued FixedSizeList
/// columns.
#[pyfunction]
#[pyo3(signature = (source, progress=None))]
fn ibt(py: Python<'_>, source: Py<PyAny>, progress: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    let source_bound = source.bind(py);

    let (ibt_file, file_name) = open_source(source_bound)?;

    // Build timecodes from SessionTime
    let timecodes = ibt_file
        .build_timecodes()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    // Build all channel tables
    let pa = py.import("pyarrow")?;
    let pa_table_class = pa.getattr("Table")?;
    let channels_dict = PyDict::new(py);

    let channel_batches = ibt_file
        .all_channels_to_arrow(&timecodes)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    let total = channel_batches.len();
    for (idx, (name, batch)) in channel_batches.into_iter().enumerate() {
        let py_batch = batch
            .to_pyarrow(py)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let table = pa_table_class.call_method1("from_batches", (vec![&py_batch],))?;
        channels_dict.set_item(&name, table)?;

        if let Some(ref cb) = progress {
            cb.call1(py, (idx + 1, total))?;
        }
    }

    // Build laps table
    let laps_batch = ibt_file
        .extract_laps(&timecodes)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let py_laps = laps_batch
        .to_pyarrow(py)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let laps_table = pa_table_class.call_method1("from_batches", (vec![&py_laps],))?;

    // Build metadata
    let metadata_dict = build_metadata(py, &ibt_file)?;

    // Construct LogFile
    let base_module = py.import("libibt.base")?;
    let logfile_class = base_module.getattr("LogFile")?;
    let logfile = logfile_class.call1((channels_dict, laps_table, &metadata_dict, file_name))?;

    Ok(logfile.unbind())
}

fn open_source(source: &Bound<'_, PyAny>) -> PyResult<(IbtFile, String)> {
    // String path
    if let Ok(path_str) = source.extract::<String>() {
        let ibt_file = IbtFile::open(&path_str)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("{}: {}", path_str, e)))?;
        return Ok((ibt_file, path_str));
    }

    // PathLike
    if source.hasattr("__fspath__")? {
        let path_str: String = source.call_method0("__fspath__")?.extract()?;
        let ibt_file = IbtFile::open(&path_str)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("{}: {}", path_str, e)))?;
        return Ok((ibt_file, path_str));
    }

    // Anything supporting the buffer protocol: bytes, bytearray, memoryview, ...
    if let Ok(buffer) = pyo3::buffer::PyBuffer::<u8>::get(source) {
        let bytes_val = buffer.to_vec(source.py())?;
        let ibt_file = IbtFile::from_bytes(bytes_val)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok((ibt_file, "<bytes>".to_string()));
    }

    // File-like with read()
    if source.hasattr("read")? {
        source.call_method1("seek", (0,))?;
        let data = source.call_method0("read")?;
        let bytes_val: Vec<u8> = data.extract()?;
        let ibt_file = IbtFile::from_bytes(bytes_val)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok((ibt_file, "<bytes>".to_string()));
    }

    Err(pyo3::exceptions::PyTypeError::new_err(
        "Expected str, PathLike, bytes-like, or file-like object",
    ))
}

/// The metadata dict, as laid out by `IbtFile::metadata_entries`.
fn build_metadata<'py>(py: Python<'py>, ibt_file: &IbtFile) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in ibt_file.metadata_entries() {
        match value {
            MetadataValue::Str(v) => dict.set_item(key, v)?,
            MetadataValue::Int(v) => dict.set_item(key, v)?,
            MetadataValue::Float(v) => dict.set_item(key, v)?,
        }
    }
    Ok(dict)
}
