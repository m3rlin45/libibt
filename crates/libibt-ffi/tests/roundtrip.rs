//! Drives the C ABI exactly as a foreign caller would: opens the bundled
//! sample, pulls every channel and the laps through `ArrowArrayStream`, and
//! checks the result against the core crate on the same file.
//!
//! Parity with the Python `LogFile` (every fixture, the metadata document
//! included) is asserted from Python, in `tests/test_ffi.py`.

use std::ffi::{c_char, CStr, CString};
use std::path::PathBuf;

use arrow::array::RecordBatch;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use ibt_ffi::*;
use libibt::IbtFile;

fn sample_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/test_data/test.ibt")
}

unsafe fn read_stream(stream: FFI_ArrowArrayStream) -> RecordBatch {
    let mut reader = ArrowArrayStreamReader::try_new(stream).expect("import stream");
    let batch = reader.next().expect("one batch").expect("batch ok");
    assert!(reader.next().is_none(), "exactly one batch per stream");
    batch
}

unsafe fn take_string(s: *mut c_char) -> String {
    assert!(!s.is_null());
    let text = CStr::from_ptr(s).to_str().unwrap().to_string();
    ibt_string_free(s);
    text
}

fn keys_of(document: &serde_json::Value) -> Vec<&str> {
    document
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect()
}

#[test]
fn c_abi_round_trip_matches_the_core_crate() {
    let path = sample_path();
    let reference = IbtFile::open(&path).expect("sample file");
    let timecodes = reference.build_timecodes().unwrap();
    let reference_batches = reference.all_channels_to_arrow(&timecodes).unwrap();
    let reference_laps = reference.extract_laps(&timecodes).unwrap();

    unsafe {
        let c_path = CString::new(path.to_str().unwrap()).unwrap();
        let mut err: *mut c_char = std::ptr::null_mut();
        let session = ibt_open_path(c_path.as_ptr(), &mut err);
        assert!(!session.is_null(), "open failed: {:?}", CStr::from_ptr(err));
        assert!(err.is_null());

        // One channel per variable, in file order.
        let count = ibt_channel_count(session);
        assert_eq!(count, reference_batches.len());
        assert_eq!(count, reference.var_headers.len());

        let mut batches = Vec::with_capacity(count);
        for i in 0..count {
            let name = take_string(ibt_channel_name(session, i));
            let mut stream = FFI_ArrowArrayStream::empty();
            assert_eq!(ibt_channel_stream(session, i, &mut stream), IBT_OK);
            batches.push((name, read_stream(stream)));
        }

        for ((name, batch), (reference_name, reference_batch)) in
            batches.iter().zip(&reference_batches)
        {
            assert_eq!(name, reference_name);
            let schema = batch.schema();
            assert_eq!(schema.field(0).name(), "timecodes");
            assert_eq!(schema.field(1).name(), name);
            // Schema (names, types, field metadata) and every value.
            assert_eq!(batch, reference_batch, "{name}");
        }

        // Out-of-range and NULL arguments are reported, and leave `out` alone.
        let mut untouched = FFI_ArrowArrayStream::empty();
        assert_eq!(
            ibt_channel_stream(session, count, &mut untouched),
            IBT_ERR_INDEX
        );
        assert!(untouched.release.is_none());
        assert_eq!(
            ibt_channel_stream(session, 0, std::ptr::null_mut()),
            IBT_ERR_NULL
        );
        assert_eq!(
            ibt_channel_stream(std::ptr::null(), 0, &mut untouched),
            IBT_ERR_NULL
        );
        assert_eq!(ibt_laps_stream(session, std::ptr::null_mut()), IBT_ERR_NULL);
        assert_eq!(
            ibt_laps_stream(std::ptr::null(), &mut untouched),
            IBT_ERR_NULL
        );
        assert!(untouched.release.is_none());
        assert!(ibt_channel_name(session, count).is_null());

        let mut laps_stream = FFI_ArrowArrayStream::empty();
        assert_eq!(ibt_laps_stream(session, &mut laps_stream), IBT_OK);
        let laps = read_stream(laps_stream);
        assert_eq!(laps, reference_laps);
        assert!(laps.num_rows() > 0);

        let json = take_string(ibt_metadata_json(session));
        let document: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(document, metadata_to_json(&reference).unwrap());
        assert_eq!(
            document["session_info_yaml"].as_str(),
            Some(reference.session_info_yaml())
        );
        assert_eq!(
            document["tick_rate"].as_i64(),
            Some(reference.tick_rate().into())
        );
        assert_eq!(
            document["record_count"].as_u64(),
            Some(reference.record_count() as u64)
        );
        assert_eq!(
            document["start_time"].as_f64(),
            Some(reference.sub_header.start_time)
        );
        // The parsed session info and the entries derived from it are there,
        // typed as the YAML resolves: numbers, nulls and an ISO date string.
        let weekend = &document["session_info"]["WeekendInfo"];
        assert_eq!(weekend["TrackName"], document["track_name"]);
        assert!(weekend["TrackID"].is_i64());
        assert_eq!(weekend["TrackID"], document["track_id"]);
        let date = weekend["WeekendOptions"]["Date"].as_str().unwrap();
        assert_eq!(date.len(), 10);
        assert_eq!(date.matches('-').count(), 2);
        assert!(document["driver_name"].is_string());
        assert!(document["num_drivers"].as_i64().unwrap() > 0);
        assert!(document["car_setup"].is_object());
        assert!(document["sectors"].is_array());
        assert_eq!(keys_of(&document).last(), Some(&"sectors"));

        // The keys come out in the document's order, not sorted.
        let keys = keys_of(&document);
        let entries = reference.metadata_entries().unwrap();
        let expected: Vec<&str> = entries.iter().map(|(key, _)| *key).collect();
        assert_eq!(keys, expected);
        assert!(keys.windows(2).any(|pair| pair[0] > pair[1]));

        // A stream exported before the session is closed outlives it.
        let mut stream = FFI_ArrowArrayStream::empty();
        assert_eq!(ibt_channel_stream(session, 0, &mut stream), IBT_OK);
        ibt_close(session);
        assert_eq!(read_stream(stream), batches[0].1);
    }
}

#[test]
fn open_bytes_matches_open_path() {
    let path = sample_path();
    let bytes = std::fs::read(&path).expect("sample file");
    unsafe {
        let mut err: *mut c_char = std::ptr::null_mut();
        let from_bytes = ibt_open_bytes(bytes.as_ptr(), bytes.len(), &mut err);
        assert!(!from_bytes.is_null() && err.is_null());
        // The bytes are only read during the call.
        drop(bytes);
        let c_path = CString::new(path.to_str().unwrap()).unwrap();
        let from_path = ibt_open_path(c_path.as_ptr(), &mut err);
        assert!(!from_path.is_null() && err.is_null());

        let count = ibt_channel_count(from_bytes);
        assert_eq!(count, ibt_channel_count(from_path));
        for i in 0..count {
            let mut a = FFI_ArrowArrayStream::empty();
            let mut b = FFI_ArrowArrayStream::empty();
            assert_eq!(ibt_channel_stream(from_bytes, i, &mut a), IBT_OK);
            assert_eq!(ibt_channel_stream(from_path, i, &mut b), IBT_OK);
            assert_eq!(read_stream(a), read_stream(b));
        }
        let mut a = FFI_ArrowArrayStream::empty();
        let mut b = FFI_ArrowArrayStream::empty();
        assert_eq!(ibt_laps_stream(from_bytes, &mut a), IBT_OK);
        assert_eq!(ibt_laps_stream(from_path, &mut b), IBT_OK);
        assert_eq!(read_stream(a), read_stream(b));
        assert_eq!(
            take_string(ibt_metadata_json(from_bytes)),
            take_string(ibt_metadata_json(from_path))
        );
        ibt_close(from_bytes);
        ibt_close(from_path);
    }
}

#[test]
fn errors_are_reported_not_thrown() {
    unsafe {
        let mut err: *mut c_char = std::ptr::null_mut();
        let missing = CString::new("/nonexistent/file.ibt").unwrap();
        assert!(ibt_open_path(missing.as_ptr(), &mut err).is_null());
        assert!(take_string(err).contains("/nonexistent/file.ibt"));

        // A NULL error out-parameter is allowed.
        assert!(ibt_open_path(missing.as_ptr(), std::ptr::null_mut()).is_null());
        assert!(ibt_open_path(std::ptr::null(), &mut err).is_null());
        assert_eq!(take_string(err), "path is NULL");

        // Bytes that are not an IBT file are rejected with a message, never
        // a panic. An empty buffer may be passed as NULL (what a C# `fixed`
        // over an empty span yields); NULL with a length is a caller bug.
        let garbage = [1u8, 2, 3, 4];
        for (data, len) in [
            (garbage.as_ptr(), garbage.len()),
            (garbage.as_ptr(), 0),
            (std::ptr::null(), 0),
        ] {
            let mut err: *mut c_char = std::ptr::null_mut();
            assert!(ibt_open_bytes(data, len, &mut err).is_null());
            let message = take_string(err);
            assert!(!message.is_empty());
            assert_ne!(message, "data is NULL");
        }

        let mut err: *mut c_char = std::ptr::null_mut();
        assert!(ibt_open_bytes(std::ptr::null(), 4, &mut err).is_null());
        assert_eq!(take_string(err), "data is NULL");

        assert_eq!(ibt_channel_count(std::ptr::null()), 0);
        assert!(ibt_channel_name(std::ptr::null(), 0).is_null());
        assert_eq!(take_string(ibt_metadata_json(std::ptr::null())), "{}");
        ibt_close(std::ptr::null_mut());
        ibt_string_free(std::ptr::null_mut());
        assert_eq!(
            CStr::from_ptr(ibt_version()).to_str().unwrap(),
            env!("CARGO_PKG_VERSION")
        );
    }
}
