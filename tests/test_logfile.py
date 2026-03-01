"""Tests for LogFile API methods."""

import numpy as np
import pyarrow as pa
import pytest
from libibt import ibt

TEST_FILE = "tests/test_data/test.ibt"


# --- select_channels ---


def test_select_channels_valid():
    log = ibt(TEST_FILE)
    sel = log.select_channels(["Speed", "Lap"])
    assert sorted(sel.channels.keys()) == ["Lap", "Speed"]
    assert len(sel.channels["Speed"]) == 65642


def test_select_channels_preserves_metadata():
    log = ibt(TEST_FILE)
    sel = log.select_channels(["Speed"])
    field = sel.channels["Speed"].schema.field("Speed")
    assert field.metadata[b"units"] == b"m/s"


def test_select_channels_preserves_laps():
    log = ibt(TEST_FILE)
    sel = log.select_channels(["Speed"])
    assert len(sel.laps) == 12


def test_select_channels_missing_raises():
    log = ibt(TEST_FILE)
    with pytest.raises(KeyError, match="NotAChannel"):
        log.select_channels(["Speed", "NotAChannel"])


# --- filter_by_time_range ---


def test_filter_by_time_range():
    log = ibt(TEST_FILE)
    filtered = log.filter_by_time_range(100000, 200000)
    for name, table in filtered.channels.items():
        tc = table.column("timecodes").to_pylist()
        assert all(100000 <= t < 200000 for t in tc), f"Channel {name} has out-of-range timecodes"
        assert len(tc) > 0, f"Channel {name} is empty after filtering"


def test_filter_by_time_range_with_channels():
    log = ibt(TEST_FILE)
    filtered = log.filter_by_time_range(100000, 200000, channel_names=["Speed", "Lap"])
    assert sorted(filtered.channels.keys()) == ["Lap", "Speed"]


def test_filter_by_time_range_preserves_metadata():
    log = ibt(TEST_FILE)
    filtered = log.filter_by_time_range(100000, 200000)
    field = filtered.channels["Speed"].schema.field("Speed")
    assert field.metadata[b"units"] == b"m/s"


def test_filter_by_time_range_filters_laps():
    log = ibt(TEST_FILE)
    # Use a range that excludes some laps
    first_lap_end = log.laps.column("end_time")[0].as_py()
    filtered = log.filter_by_time_range(first_lap_end + 1, first_lap_end + 10000)
    # Should not include the first lap since its end_time < start_time
    assert len(filtered.laps) < len(log.laps)


# --- filter_by_lap ---


def test_filter_by_lap_valid():
    log = ibt(TEST_FILE)
    lap3 = log.filter_by_lap(3)
    assert len(lap3.channels["Speed"]) > 0
    # All timecodes should be within lap 3's time range
    lap3_start = log.laps.column("start_time")[3].as_py()
    lap3_end = log.laps.column("end_time")[3].as_py()
    tc = lap3.channels["Speed"].column("timecodes").to_pylist()
    assert all(lap3_start <= t < lap3_end for t in tc)


def test_filter_by_lap_with_channels():
    log = ibt(TEST_FILE)
    lap3 = log.filter_by_lap(3, channel_names=["Speed"])
    assert list(lap3.channels.keys()) == ["Speed"]


def test_filter_by_lap_invalid_raises():
    log = ibt(TEST_FILE)
    with pytest.raises(ValueError, match="Lap 99 not found"):
        log.filter_by_lap(99)


def test_filter_by_lap_rows_consistent():
    log = ibt(TEST_FILE)
    lap0 = log.filter_by_lap(0)
    # Every channel should have the same number of rows (same timebase)
    row_counts = {name: len(t) for name, t in lap0.channels.items()}
    counts = set(row_counts.values())
    assert len(counts) == 1, f"Inconsistent row counts across channels in lap 0: {counts}"


# --- get_channels_as_table ---


def test_get_channels_as_table():
    log = ibt(TEST_FILE)
    merged = log.get_channels_as_table()
    # Should have timecodes + 273 channels = 274 columns
    assert merged.num_columns == 274
    assert len(merged) == 65642
    assert "timecodes" in merged.column_names
    assert "Speed" in merged.column_names


def test_get_channels_as_table_preserves_metadata():
    log = ibt(TEST_FILE)
    merged = log.get_channels_as_table()
    field = merged.schema.field("Speed")
    assert field.metadata is not None
    assert field.metadata[b"units"] == b"m/s"


def test_get_channels_as_table_empty():
    log = ibt(TEST_FILE)
    empty = log.select_channels([])
    # select_channels raises KeyError for empty... let's test the empty path differently
    from libibt.base import LogFile

    empty_log = LogFile(channels={}, laps=log.laps, metadata=log.metadata, file_name=log.file_name)
    merged = empty_log.get_channels_as_table()
    assert merged.num_columns == 1  # just timecodes
    assert len(merged) == 0


# --- resample_to_channel ---


def test_resample_to_channel():
    log = ibt(TEST_FILE)
    resampled = log.resample_to_channel("Speed", channel_names=["Speed", "Lap"])
    assert sorted(resampled.channels.keys()) == ["Lap", "Speed"]
    # All channels should share the same timecodes as Speed
    speed_tc = resampled.channels["Speed"].column("timecodes").to_pylist()
    lap_tc = resampled.channels["Lap"].column("timecodes").to_pylist()
    assert speed_tc == lap_tc


def test_resample_to_channel_missing_raises():
    log = ibt(TEST_FILE)
    with pytest.raises(KeyError, match="NotAChannel"):
        log.resample_to_channel("NotAChannel")


def test_resample_to_channel_preserves_row_count():
    log = ibt(TEST_FILE)
    resampled = log.resample_to_channel("Speed")
    for name, table in resampled.channels.items():
        assert len(table) == 65642, f"Channel {name} has {len(table)} rows after resample"


# --- resample_to_timecodes ---


def test_resample_to_timecodes():
    log = ibt(TEST_FILE)
    target_tc = pa.array([100000, 200000, 300000], type=pa.int64())
    resampled = log.resample_to_timecodes(target_tc, channel_names=["Speed", "Lap"])
    assert sorted(resampled.channels.keys()) == ["Lap", "Speed"]
    for name, table in resampled.channels.items():
        assert len(table) == 3
        assert table.column("timecodes").to_pylist() == [100000, 200000, 300000]


def test_resample_interpolated_channel():
    log = ibt(TEST_FILE)
    # Speed has interpolate=True, so should use linear interpolation
    tc = log.channels["Speed"].column("timecodes").to_pylist()
    mid = (tc[0] + tc[1]) // 2  # midpoint between first two samples
    target_tc = pa.array([mid], type=pa.int64())
    resampled = log.resample_to_timecodes(target_tc, channel_names=["Speed"])
    val = resampled.channels["Speed"].column("Speed")[0].as_py()
    # Should be interpolated between the first two Speed values
    v0 = log.channels["Speed"].column("Speed")[0].as_py()
    v1 = log.channels["Speed"].column("Speed")[1].as_py()
    assert min(v0, v1) <= val <= max(v0, v1) or val == pytest.approx((v0 + v1) / 2, rel=0.1)


# --- Lap classification ---

MULTI_SESSION_FILE = "tests/test_data/formulair04_bathurst 2026-02-21 20-57-19.ibt"


def test_lap_type_column_exists():
    log = ibt(TEST_FILE)
    assert "lap_type" in log.laps.column_names


def test_session_column_exists():
    log = ibt(TEST_FILE)
    assert "session" in log.laps.column_names


def test_real_file_lap_types():
    log = ibt(TEST_FILE)
    types = log.laps.column("lap_type").to_pylist()
    valid_types = {"full", "out", "in", "partial", "incomplete"}
    assert all(t in valid_types for t in types)
    # test.ibt is a race file — should have mostly full laps
    assert types.count("full") >= 10


def test_bathurst_lap_types():
    log = ibt(MULTI_SESSION_FILE)
    types = log.laps.column("lap_type").to_pylist()
    # Should have a mix of types
    assert "full" in types
    assert "partial" in types or "incomplete" in types or "out" in types


def test_bathurst_session_count():
    log = ibt(MULTI_SESSION_FILE)
    sessions = log.laps.column("session").to_pylist()
    assert max(sessions) >= 1  # At least 2 sessions (0 and 1)


def test_lap_type_preserved_through_filter():
    log = ibt(TEST_FILE)
    filtered = log.filter_by_time_range(100000, 200000)
    assert "lap_type" in filtered.laps.column_names
    assert "session" in filtered.laps.column_names


def test_lap_type_preserved_through_select():
    log = ibt(TEST_FILE)
    sel = log.select_channels(["Speed"])
    assert "lap_type" in sel.laps.column_names
    assert "session" in sel.laps.column_names


# --- split_sessions ---


def test_split_no_resets():
    log = ibt(TEST_FILE)
    sessions = log.split_sessions()
    assert len(sessions) == 1
    assert len(sessions[0].laps) > 0


def test_split_bathurst():
    log = ibt(MULTI_SESSION_FILE)
    sessions = log.split_sessions()
    assert len(sessions) >= 2


def test_split_drops_incomplete():
    log = ibt(MULTI_SESSION_FILE)
    sessions = log.split_sessions()
    for s in sessions:
        types = s.laps.column("lap_type").to_pylist()
        assert "incomplete" not in types


def test_split_channels_time_bounded():
    log = ibt(MULTI_SESSION_FILE)
    sessions = log.split_sessions()
    for s in sessions:
        start = int(s.laps.column("start_time")[0].as_py())
        end = int(s.laps.column("end_time")[-1].as_py())
        for name, table in s.channels.items():
            tc = table.column("timecodes").to_pylist()
            if tc:
                assert tc[0] >= start, f"Channel {name} starts before session"
                assert tc[-1] < end, f"Channel {name} ends after session"


def test_split_metadata_independent():
    log = ibt(MULTI_SESSION_FILE)
    sessions = log.split_sessions()
    if len(sessions) >= 2:
        sessions[0].metadata["test_key"] = "test_value"
        assert "test_key" not in sessions[1].metadata


def test_split_then_filter_by_lap():
    log = ibt(MULTI_SESSION_FILE)
    sessions = log.split_sessions()
    # Filter by a lap in the first session
    first = sessions[0]
    lap_nums = first.laps.column("num").to_pylist()
    if lap_nums:
        filtered = first.filter_by_lap(lap_nums[0])
        assert len(filtered.channels) > 0
