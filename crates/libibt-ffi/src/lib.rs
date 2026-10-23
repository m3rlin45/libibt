//! C ABI for libibt.
//!
//! A session is parsed once into Arrow record batches (one per scalar
//! channel, one for the laps table) and handed out as an opaque
//! [`IbtSession`] pointer. Channels and laps are exported through the Arrow
//! C Data Interface (`ArrowArrayStream`); metadata is exported as a JSON
//! document with the same keys and value types as the Python
//! `LogFile.metadata` dict built by libibt-python.
//!
//! Conventions: every function is `unsafe extern "C"` and panic-safe (panics
//! are caught and reported as errors). Strings returned as `*mut c_char` are
//! owned by the caller and must be released with [`ibt_string_free`]. Error
//! out-parameters receive such a string, or NULL on success.

use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use arrow::array::{RecordBatch, RecordBatchIterator};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use libibt::IbtFile;
use serde_json::{json, Map, Value};

/// A parsed session: scalar channel batches in file order, the laps batch
/// and the metadata JSON.
pub struct IbtSession {
    channels: Vec<(String, RecordBatch)>,
    laps: RecordBatch,
    metadata_json: CString,
}

/// Build a session from an opened file.
pub fn open_session(file: &IbtFile) -> Result<IbtSession, String> {
    let timecodes = file.build_timecodes().map_err(|e| e.to_string())?;
    let channels = file
        .all_channels_to_arrow(&timecodes)
        .map_err(|e| e.to_string())?;
    let laps = file.extract_laps(&timecodes).map_err(|e| e.to_string())?;
    let metadata_json =
        CString::new(metadata_to_json(file).to_string()).map_err(|e| e.to_string())?;
    Ok(IbtSession {
        channels,
        laps,
        metadata_json,
    })
}

/// The metadata as JSON with the Python dict's keys and value types (see
/// libibt-python's `build_metadata`): the raw session-info YAML, a few
/// track/session fields scanned from it, and the header counters.
pub fn metadata_to_json(file: &IbtFile) -> Value {
    let yaml = file.session_info_yaml();
    let mut m = Map::new();
    m.insert("session_info_yaml".into(), Value::String(yaml.to_string()));

    const SCANNED: &[(&str, &str)] = &[
        ("TrackName: ", "track_name"),
        ("TrackDisplayName: ", "track_display_name"),
        ("TrackCity: ", "track_city"),
        ("TrackCountry: ", "track_country"),
        ("TrackLength: ", "track_length"),
        ("SeriesID: ", "series_id"),
        ("SeasonID: ", "season_id"),
        ("SessionID: ", "session_id"),
        ("SubSessionID: ", "sub_session_id"),
    ];
    for line in yaml.lines() {
        let trimmed = line.trim();
        for (prefix, key) in SCANNED {
            if let Some(rest) = trimmed.strip_prefix(prefix) {
                m.insert((*key).into(), Value::String(rest.trim().to_string()));
                break;
            }
        }
    }

    m.insert("tick_rate".into(), json!(file.tick_rate()));
    m.insert("record_count".into(), json!(file.record_count()));
    m.insert("lap_count".into(), json!(file.sub_header.lap_count));
    m.insert(
        "session_start_date".into(),
        json!(file.sub_header.session_start_date),
    );
    m.insert("start_time".into(), json!(file.sub_header.start_time));
    m.insert("end_time".into(), json!(file.sub_header.end_time));
    Value::Object(m)
}

impl IbtSession {
    /// Channel names in export order.
    pub fn channel_names(&self) -> impl Iterator<Item = &str> {
        self.channels.iter().map(|(name, _)| name.as_str())
    }

    /// The batch of channel `i`.
    pub fn channel(&self, i: usize) -> Option<&RecordBatch> {
        self.channels.get(i).map(|(_, b)| b)
    }

    /// The laps batch.
    pub fn laps(&self) -> &RecordBatch {
        &self.laps
    }

    /// The metadata JSON text.
    pub fn metadata_json(&self) -> &str {
        self.metadata_json.to_str().unwrap_or("{}")
    }
}

fn batch_stream(batch: &RecordBatch) -> FFI_ArrowArrayStream {
    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
    FFI_ArrowArrayStream::new(Box::new(reader))
}

fn owned_string(s: &str) -> *mut c_char {
    CString::new(s.replace('\0', "")).map_or(ptr::null_mut(), CString::into_raw)
}

unsafe fn set_error(err: *mut *mut c_char, message: &str) {
    if !err.is_null() {
        *err = owned_string(message);
    }
}

unsafe fn open_guarded(
    err: *mut *mut c_char,
    open: impl FnOnce() -> Result<IbtFile, String>,
) -> *mut IbtSession {
    if !err.is_null() {
        *err = ptr::null_mut();
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        open().and_then(|file| open_session(&file))
    }));
    match outcome {
        Ok(Ok(session)) => Box::into_raw(Box::new(session)),
        Ok(Err(message)) => {
            set_error(err, &message);
            ptr::null_mut()
        }
        Err(_) => {
            set_error(err, "libibt panicked while parsing the file");
            ptr::null_mut()
        }
    }
}

/// Parse the file at `path` (UTF-8). Returns NULL and sets `*err` on failure.
///
/// # Safety
/// `path` must be a valid NUL-terminated string; `err` may be NULL.
#[no_mangle]
pub unsafe extern "C" fn ibt_open_path(
    path: *const c_char,
    err: *mut *mut c_char,
) -> *mut IbtSession {
    open_guarded(err, || {
        if path.is_null() {
            return Err("path is NULL".into());
        }
        let path = CStr::from_ptr(path).to_str().map_err(|e| e.to_string())?;
        IbtFile::open(path).map_err(|e| format!("{}: {}", path, e))
    })
}

/// Parse `len` bytes at `data`. Returns NULL and sets `*err` on failure.
///
/// # Safety
/// `data` must point to `len` readable bytes; `err` may be NULL.
#[no_mangle]
pub unsafe extern "C" fn ibt_open_bytes(
    data: *const u8,
    len: usize,
    err: *mut *mut c_char,
) -> *mut IbtSession {
    open_guarded(err, || {
        if data.is_null() {
            return Err("data is NULL".into());
        }
        IbtFile::from_bytes(std::slice::from_raw_parts(data, len).to_vec())
            .map_err(|e| e.to_string())
    })
}

/// Release a session. NULL is ignored.
///
/// # Safety
/// `session` must come from `ibt_open_*` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn ibt_close(session: *mut IbtSession) {
    if !session.is_null() {
        drop(Box::from_raw(session));
    }
}

/// Number of scalar channels.
///
/// # Safety
/// `session` must be a live session pointer.
#[no_mangle]
pub unsafe extern "C" fn ibt_channel_count(session: *const IbtSession) -> usize {
    session.as_ref().map_or(0, |s| s.channels.len())
}

/// The name of channel `index` as an owned string, or NULL when out of range.
///
/// # Safety
/// `session` must be a live session pointer.
#[no_mangle]
pub unsafe extern "C" fn ibt_channel_name(session: *const IbtSession, index: usize) -> *mut c_char {
    match session.as_ref().and_then(|s| s.channels.get(index)) {
        Some((name, _)) => owned_string(name),
        None => ptr::null_mut(),
    }
}

/// The metadata JSON document as an owned string.
///
/// # Safety
/// `session` must be a live session pointer.
#[no_mangle]
pub unsafe extern "C" fn ibt_metadata_json(session: *const IbtSession) -> *mut c_char {
    match session.as_ref() {
        Some(s) => owned_string(s.metadata_json()),
        None => owned_string("{}"),
    }
}

/// Export channel `index` into `out` as an Arrow array stream of one record
/// batch (`timecodes` Int64 ms, then the channel's values; field metadata
/// carries units, desc, interpolate). Returns 0 on success, 1 when `index`
/// is out of range, 2 for a NULL argument.
///
/// # Safety
/// `out` must point to an `ArrowArrayStream` the caller owns and will release.
#[no_mangle]
pub unsafe extern "C" fn ibt_channel_stream(
    session: *const IbtSession,
    index: usize,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    let Some(session) = session.as_ref() else {
        return 2;
    };
    if out.is_null() {
        return 2;
    }
    match session.channels.get(index) {
        Some((_, batch)) => {
            ptr::write(out, batch_stream(batch));
            0
        }
        None => 1,
    }
}

/// Export the laps table (`num` Int32, `start_time`/`end_time` Int64 ms,
/// `lap_type` Utf8, `session` Int32) into `out`. Returns 0 on success, 2
/// for a NULL argument.
///
/// # Safety
/// `out` must point to an `ArrowArrayStream` the caller owns and will release.
#[no_mangle]
pub unsafe extern "C" fn ibt_laps_stream(
    session: *const IbtSession,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    let Some(session) = session.as_ref() else {
        return 2;
    };
    if out.is_null() {
        return 2;
    }
    ptr::write(out, batch_stream(&session.laps));
    0
}

/// Release a string returned by this library. NULL is ignored.
///
/// # Safety
/// `s` must come from this library and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn ibt_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}

/// This crate's version as a static string (do not free).
#[no_mangle]
pub extern "C" fn ibt_version() -> *const c_char {
    static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");
    VERSION.as_ptr() as *const c_char
}
