use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use libibt::IbtFile;

/// iRacing IBT telemetry file parser implemented in Rust.
#[pymodule]
fn _libibt_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ibt, m)?)?;
    Ok(())
}

/// Parse an iRacing IBT file and return a LogFile.
///
/// Note: Array variables (those with count > 1, e.g. tire temp arrays)
/// are not included as channels. Only scalar (count == 1) variables are
/// returned.
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

    // Bytes
    if let Ok(bytes_val) = source.extract::<Vec<u8>>() {
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
        "Expected str, bytes, PathLike, or file-like object",
    ))
}

fn build_metadata<'py>(py: Python<'py>, ibt_file: &IbtFile) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let yaml_str = ibt_file.session_info_yaml();

    dict.set_item("session_info_yaml", yaml_str)?;

    for line in yaml_str.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("TrackName: ") {
            dict.set_item("track_name", rest.trim())?;
        } else if let Some(rest) = trimmed.strip_prefix("TrackDisplayName: ") {
            dict.set_item("track_display_name", rest.trim())?;
        } else if let Some(rest) = trimmed.strip_prefix("TrackCity: ") {
            dict.set_item("track_city", rest.trim())?;
        } else if let Some(rest) = trimmed.strip_prefix("TrackCountry: ") {
            dict.set_item("track_country", rest.trim())?;
        } else if let Some(rest) = trimmed.strip_prefix("TrackLength: ") {
            dict.set_item("track_length", rest.trim())?;
        } else if let Some(rest) = trimmed.strip_prefix("SeriesID: ") {
            dict.set_item("series_id", rest.trim())?;
        } else if let Some(rest) = trimmed.strip_prefix("SeasonID: ") {
            dict.set_item("season_id", rest.trim())?;
        } else if let Some(rest) = trimmed.strip_prefix("SessionID: ") {
            dict.set_item("session_id", rest.trim())?;
        } else if let Some(rest) = trimmed.strip_prefix("SubSessionID: ") {
            dict.set_item("sub_session_id", rest.trim())?;
        }
    }

    dict.set_item("tick_rate", ibt_file.tick_rate())?;
    dict.set_item("record_count", ibt_file.record_count())?;
    dict.set_item("lap_count", ibt_file.sub_header.lap_count)?;
    dict.set_item("session_start_date", ibt_file.sub_header.session_start_date)?;
    dict.set_item("start_time", ibt_file.sub_header.start_time)?;
    dict.set_item("end_time", ibt_file.sub_header.end_time)?;

    Ok(dict)
}
