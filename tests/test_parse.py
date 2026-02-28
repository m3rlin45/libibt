"""Tests for basic IBT file parsing."""

import pyarrow as pa
from libibt import ibt

TEST_FILE = "tests/test_data/test.ibt"


def test_channel_count():
    log = ibt(TEST_FILE)
    assert len(log.channels) == 273


def test_record_count_per_channel():
    log = ibt(TEST_FILE)
    for name, table in log.channels.items():
        assert len(table) == 65642, f"Channel {name} has {len(table)} rows, expected 65642"


def test_tick_rate():
    log = ibt(TEST_FILE)
    assert log.metadata["tick_rate"] == 60


def test_lap_count():
    log = ibt(TEST_FILE)
    assert len(log.laps) == 12
    assert log.metadata["lap_count"] == 12


def test_lap_numbers():
    log = ibt(TEST_FILE)
    lap_nums = log.laps.column("num").to_pylist()
    assert lap_nums == list(range(12))


def test_laps_schema():
    log = ibt(TEST_FILE)
    schema = log.laps.schema
    assert schema.field("num").type == pa.int32()
    assert schema.field("start_time").type == pa.int64()
    assert schema.field("end_time").type == pa.int64()


def test_laps_times_valid():
    log = ibt(TEST_FILE)
    starts = log.laps.column("start_time").to_pylist()
    ends = log.laps.column("end_time").to_pylist()
    for i, (s, e) in enumerate(zip(starts, ends)):
        assert s < e, f"Lap {i}: start_time {s} >= end_time {e}"


def test_metadata_keys():
    log = ibt(TEST_FILE)
    expected_keys = {
        "tick_rate",
        "record_count",
        "lap_count",
        "session_info_yaml",
        "start_time",
        "end_time",
        "track_name",
        "track_display_name",
        "track_city",
        "track_country",
        "track_length",
        "session_id",
        "sub_session_id",
        "season_id",
        "series_id",
        "session_start_date",
    }
    assert expected_keys == set(log.metadata.keys())


def test_metadata_record_count():
    log = ibt(TEST_FILE)
    assert log.metadata["record_count"] == 65642


def test_file_name():
    log = ibt(TEST_FILE)
    assert log.file_name == TEST_FILE


def test_each_channel_has_timecodes():
    log = ibt(TEST_FILE)
    for name, table in log.channels.items():
        assert "timecodes" in table.column_names, f"Channel {name} missing timecodes column"
        assert name in table.column_names, f"Channel {name} missing value column"
