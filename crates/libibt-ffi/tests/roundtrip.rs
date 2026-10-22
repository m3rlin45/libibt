//! Drives the C ABI as a foreign caller would on the bundled sample and
//! checks the result against the core API on the same file.

use std::ffi::{c_char, CStr, CString};
use std::path::PathBuf;

use arrow::array::{Array, Int64Array};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use ibt_ffi::*;
use libibt::IbtFile;

fn sample_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/test_data/test.ibt")
}

unsafe fn read_stream(stream: FFI_ArrowArrayStream) -> arrow::array::RecordBatch {
    let mut reader = ArrowArrayStreamReader::try_new(stream).expect("import stream");
    let batch = reader.next().expect("one batch").expect("batch ok");
    assert!(reader.next().is_none(), "exactly one batch per stream");
    batch
}

#[test]
fn c_abi_round_trip_matches_core() {
    let path = sample_path();
    let reference = IbtFile::open(&path).expect("sample file");
    let timecodes = reference.build_timecodes().unwrap();
    let reference_channels = reference.all_channels_to_arrow(&timecodes).unwrap();
    let reference_laps = reference.extract_laps(&timecodes).unwrap();

    unsafe {
        let c_path = CString::new(path.to_str().unwrap()).unwrap();
        let mut err: *mut c_char = std::ptr::null_mut();
        let session = ibt_open_path(c_path.as_ptr(), &mut err);
        assert!(!session.is_null(), "open failed: {:?}", CStr::from_ptr(err));
        assert!(err.is_null());

        let count = ibt_channel_count(session);
        assert_eq!(count, reference_channels.len());
        for (i, (expected_name, expected_batch)) in reference_channels.iter().enumerate() {
            let name_ptr = ibt_channel_name(session, i);
            let name = CStr::from_ptr(name_ptr).to_str().unwrap().to_string();
            ibt_string_free(name_ptr);
            assert_eq!(&name, expected_name);

            let mut stream = FFI_ArrowArrayStream::empty();
            assert_eq!(ibt_channel_stream(session, i, &mut stream), 0);
            let batch = read_stream(stream);
            assert_eq!(batch.schema().field(0).name(), "timecodes");
            assert_eq!(batch.schema().field(1).name(), &name);
            assert_eq!(batch.num_rows(), expected_batch.num_rows());
            let t = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let t_ref = expected_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(t.values().last(), t_ref.values().last());
        }
        assert_eq!(
            ibt_channel_stream(session, count, &mut FFI_ArrowArrayStream::empty()),
            1
        );

        let mut laps_stream = FFI_ArrowArrayStream::empty();
        assert_eq!(ibt_laps_stream(session, &mut laps_stream), 0);
        let laps = read_stream(laps_stream);
        assert_eq!(laps.num_rows(), reference_laps.num_rows());
        assert_eq!(
            laps.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            ["num", "start_time", "end_time", "lap_type", "session"]
        );

        let json_ptr = ibt_metadata_json(session);
        let json: serde_json::Value =
            serde_json::from_str(CStr::from_ptr(json_ptr).to_str().unwrap()).unwrap();
        ibt_string_free(json_ptr);
        assert!(json["session_info_yaml"]
            .as_str()
            .unwrap()
            .contains("TrackName"));
        assert_eq!(
            json["tick_rate"].as_i64(),
            Some(reference.tick_rate() as i64)
        );
        assert_eq!(
            json["record_count"].as_u64(),
            Some(reference.record_count() as u64)
        );

        ibt_close(session);
    }
}

#[test]
fn errors_are_reported_not_thrown() {
    unsafe {
        let mut err: *mut c_char = std::ptr::null_mut();
        let missing = CString::new("/nonexistent/file.ibt").unwrap();
        assert!(ibt_open_path(missing.as_ptr(), &mut err).is_null());
        assert!(!err.is_null());
        ibt_string_free(err);

        let garbage = [1u8, 2, 3, 4];
        let mut err: *mut c_char = std::ptr::null_mut();
        assert!(ibt_open_bytes(garbage.as_ptr(), garbage.len(), &mut err).is_null());
        assert!(!err.is_null());
        ibt_string_free(err);

        assert_eq!(ibt_channel_count(std::ptr::null()), 0);
        ibt_close(std::ptr::null_mut());
        assert!(!CStr::from_ptr(ibt_version()).to_str().unwrap().is_empty());
    }
}
