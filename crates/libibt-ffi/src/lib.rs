//! C ABI for libibt.
//!
//! A session is parsed once into Arrow record batches (one per channel, one
//! for the laps table) and handed out as an opaque [`IbtSession`] pointer.
//! Channels and laps are exported through the Arrow C Data Interface
//! (`ArrowArrayStream`), so any Arrow implementation can consume them with no
//! copies; metadata is exported as a JSON document.
//!
//! What a session holds is what the Python `LogFile` holds: the same
//! channels in the same order, with the same schemas and field metadata, the
//! same laps table, and the `LogFile.metadata` dict
//! ([`IbtFile::metadata_entries`]) as JSON with the same keys in the same
//! order, the parsed session info and the entries derived from it included.
//! `tests/test_ffi.py` asserts that on every fixture.
//!
//! JSON has fewer types than the metadata document, so in the JSON: a date
//! or a date and time is an ISO 8601 string (`"2026-02-27"`,
//! `"2026-02-27T20:40:38+00:00"`), a non-finite number is `null`, and a
//! mapping key that is not a string (the session-info YAML could hold
//! `1: x`) is its JSON text (`"1"`).
//!
//! # Conventions
//!
//! - Every function is `unsafe extern "C"` and panic-safe: a panic is caught
//!   and reported as that function's error value.
//! - Strings returned as `*mut c_char` are owned by the caller and must be
//!   released with [`ibt_string_free`]. Error out-parameters receive such a
//!   string, or NULL on success. All strings are NUL-terminated UTF-8.
//! - A session is immutable once opened: any number of threads may read
//!   from the same session at the same time. [`ibt_close`] must not race
//!   with any other call on that session.
//! - An exported stream shares the session's buffers by reference count and
//!   stays valid after [`ibt_close`]; the memory is freed when the session
//!   and every stream exported from it have been released.

use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

use arrow::array::{RecordBatch, RecordBatchIterator};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use libibt::{IbtFile, MetadataValue};
use serde_json::{Map, Value};

/// [`ibt_channel_stream`] / [`ibt_laps_stream`]: the stream was exported.
pub const IBT_OK: i32 = 0;
/// [`ibt_channel_stream`]: `index` is not below [`ibt_channel_count`].
pub const IBT_ERR_INDEX: i32 = 1;
/// [`ibt_channel_stream`] / [`ibt_laps_stream`]: an argument was NULL.
pub const IBT_ERR_NULL: i32 = 2;
/// [`ibt_channel_stream`] / [`ibt_laps_stream`]: the library panicked.
pub const IBT_ERR_PANIC: i32 = 3;

/// A parsed session: channel batches in file order (one per variable), the
/// laps batch and the metadata JSON.
pub struct IbtSession {
    channels: Vec<(String, RecordBatch)>,
    laps: RecordBatch,
    metadata_json: String,
}

/// Build a session from an opened file. The session copies what it needs:
/// it does not keep `file` or its mapping alive.
pub fn open_session(file: &IbtFile) -> Result<IbtSession, String> {
    let timecodes = file.build_timecodes().map_err(|e| e.to_string())?;
    let channels = file
        .all_channels_to_arrow(&timecodes)
        .map_err(|e| e.to_string())?;
    let laps = file.extract_laps(&timecodes).map_err(|e| e.to_string())?;
    let metadata_json = metadata_to_json(file)?.to_string();
    Ok(IbtSession {
        channels,
        laps,
        metadata_json,
    })
}

/// The metadata document ([`IbtFile::metadata_entries`]) as a JSON object.
///
/// Keys keep the document's order. See the crate documentation for how
/// dates, non-finite numbers and non-string keys are written. Fails when
/// the file's session-info YAML cannot be parsed.
pub fn metadata_to_json(file: &IbtFile) -> Result<Value, String> {
    let entries = file.metadata_entries().map_err(|e| e.to_string())?;
    let mut map = Map::new();
    for (key, value) in &entries {
        map.insert((*key).to_string(), value_to_json(value));
    }
    Ok(Value::Object(map))
}

fn value_to_json(value: &MetadataValue) -> Value {
    match value {
        MetadataValue::Null => Value::Null,
        MetadataValue::Bool(v) => Value::Bool(*v),
        MetadataValue::Int(v) => Value::from(*v),
        // `Value::from(f64)` is null for NaN and the infinities.
        MetadataValue::Float(v) => Value::from(*v),
        MetadataValue::Str(v) => Value::String(v.clone()),
        MetadataValue::Date(v) => Value::String(v.to_string()),
        MetadataValue::DateTime(v) => Value::String(v.to_string()),
        MetadataValue::List(items) => Value::Array(items.iter().map(value_to_json).collect()),
        MetadataValue::Map(pairs) => {
            let mut map = Map::new();
            for (key, item) in pairs {
                let key = match value_to_json(key) {
                    Value::String(text) => text,
                    other => other.to_string(),
                };
                map.insert(key, value_to_json(item));
            }
            Value::Object(map)
        }
    }
}

fn batch_stream(batch: &RecordBatch) -> FFI_ArrowArrayStream {
    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
    FFI_ArrowArrayStream::new(Box::new(reader))
}

/// `s` as a caller-owned C string (an interior NUL cannot be represented and
/// is dropped), or NULL if that fails.
fn owned_string(s: &str) -> *mut c_char {
    CString::new(s.replace('\0', "")).map_or(ptr::null_mut(), CString::into_raw)
}

/// Run `f`, turning a panic into `on_panic`: unwinding must not cross the
/// `extern "C"` boundary.
fn guarded<T>(on_panic: T, f: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(on_panic)
}

unsafe fn open_guarded(
    err: *mut *mut c_char,
    open: impl FnOnce() -> Result<IbtFile, String>,
) -> *mut IbtSession {
    if !err.is_null() {
        *err = ptr::null_mut();
    }
    let outcome = guarded(
        Err("libibt panicked while parsing the file".to_string()),
        || open().and_then(|file| open_session(&file)),
    );
    match outcome {
        Ok(session) => Box::into_raw(Box::new(session)),
        Err(message) => {
            if !err.is_null() {
                *err = owned_string(&message);
            }
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

/// Parse `len` bytes at `data`. The bytes are only read during the call.
/// Returns NULL and sets `*err` on failure.
///
/// # Safety
/// `data` must point to `len` readable bytes (it may be NULL when `len` is
/// 0); `err` may be NULL.
#[no_mangle]
pub unsafe extern "C" fn ibt_open_bytes(
    data: *const u8,
    len: usize,
    err: *mut *mut c_char,
) -> *mut IbtSession {
    open_guarded(err, || {
        let bytes = if len == 0 {
            Vec::new()
        } else if data.is_null() {
            return Err("data is NULL".into());
        } else {
            std::slice::from_raw_parts(data, len).to_vec()
        };
        IbtFile::from_bytes(bytes).map_err(|e| e.to_string())
    })
}

/// Release a session. NULL is ignored.
///
/// # Safety
/// `session` must come from `ibt_open_*` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn ibt_close(session: *mut IbtSession) {
    if !session.is_null() {
        guarded((), || drop(Box::from_raw(session)));
    }
}

/// Number of channels (one per variable in the file). 0 for NULL.
///
/// # Safety
/// `session` must be NULL or a live session pointer.
#[no_mangle]
pub unsafe extern "C" fn ibt_channel_count(session: *const IbtSession) -> usize {
    session.as_ref().map_or(0, |s| s.channels.len())
}

/// The name of channel `index` as an owned string, or NULL when out of range.
///
/// # Safety
/// `session` must be NULL or a live session pointer.
#[no_mangle]
pub unsafe extern "C" fn ibt_channel_name(session: *const IbtSession, index: usize) -> *mut c_char {
    guarded(ptr::null_mut(), || {
        match session.as_ref().and_then(|s| s.channels.get(index)) {
            Some((name, _)) => owned_string(name),
            None => ptr::null_mut(),
        }
    })
}

/// The metadata JSON document as an owned string: one object with the keys
/// of the Python `LogFile.metadata` dict, in the same order (`"{}"` for
/// NULL). Dates are ISO 8601 strings and non-finite numbers are `null`.
///
/// # Safety
/// `session` must be NULL or a live session pointer.
#[no_mangle]
pub unsafe extern "C" fn ibt_metadata_json(session: *const IbtSession) -> *mut c_char {
    guarded(ptr::null_mut(), || match session.as_ref() {
        Some(s) => owned_string(&s.metadata_json),
        None => owned_string("{}"),
    })
}

/// Export channel `index` into `out` as an Arrow array stream of one record
/// batch: `timecodes` (Int64 ms), then the channel's values under its name.
/// The values are a plain column for a scalar variable and a FixedSizeList
/// for an array variable; field metadata carries units, desc and
/// interpolate. Returns [`IBT_OK`], [`IBT_ERR_INDEX`], [`IBT_ERR_NULL`] or
/// [`IBT_ERR_PANIC`]; `out` is only written on [`IBT_OK`].
///
/// # Safety
/// `session` must be NULL or a live session pointer. `out` must point to
/// writable memory for one `ArrowArrayStream`; whatever it held is
/// overwritten, not released. The caller releases the exported stream.
#[no_mangle]
pub unsafe extern "C" fn ibt_channel_stream(
    session: *const IbtSession,
    index: usize,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    guarded(IBT_ERR_PANIC, || {
        let Some(session) = session.as_ref() else {
            return IBT_ERR_NULL;
        };
        if out.is_null() {
            return IBT_ERR_NULL;
        }
        match session.channels.get(index) {
            Some((_, batch)) => {
                ptr::write(out, batch_stream(batch));
                IBT_OK
            }
            None => IBT_ERR_INDEX,
        }
    })
}

/// Export the laps table (`num` Int32, `start_time`/`end_time` Int64 ms,
/// `lap_type` Utf8, `session` Int32) into `out`. Returns [`IBT_OK`],
/// [`IBT_ERR_NULL`] or [`IBT_ERR_PANIC`]; `out` is only written on
/// [`IBT_OK`].
///
/// # Safety
/// As for [`ibt_channel_stream`].
#[no_mangle]
pub unsafe extern "C" fn ibt_laps_stream(
    session: *const IbtSession,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    guarded(IBT_ERR_PANIC, || {
        let Some(session) = session.as_ref() else {
            return IBT_ERR_NULL;
        };
        if out.is_null() {
            return IBT_ERR_NULL;
        }
        ptr::write(out, batch_stream(&session.laps));
        IBT_OK
    })
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
