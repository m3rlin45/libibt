"""Tests for channel data types, metadata, and values."""

import numpy as np
import pyarrow as pa
from libibt import ibt

TEST_FILE = "tests/test_data/test.ibt"


def test_speed_type():
    log = ibt(TEST_FILE)
    assert log.channels["Speed"].schema.field("Speed").type == pa.float32()


def test_lap_type():
    log = ibt(TEST_FILE)
    assert log.channels["Lap"].schema.field("Lap").type == pa.int32()


def test_session_time_type():
    log = ibt(TEST_FILE)
    assert log.channels["SessionTime"].schema.field("SessionTime").type == pa.float64()


def test_rpm_type():
    log = ibt(TEST_FILE)
    assert log.channels["RPM"].schema.field("RPM").type == pa.float32()


def test_gear_type():
    log = ibt(TEST_FILE)
    assert log.channels["Gear"].schema.field("Gear").type == pa.int32()


def test_timecodes_type():
    log = ibt(TEST_FILE)
    for name, table in log.channels.items():
        assert table.column("timecodes").type == pa.int64(), (
            f"Channel {name} timecodes has type {table.column('timecodes').type}, expected int64"
        )


def test_timecodes_monotonically_increasing():
    log = ibt(TEST_FILE)
    tc = log.channels["Speed"].column("timecodes").to_numpy()
    diffs = np.diff(tc)
    assert np.all(diffs > 0), "Timecodes are not monotonically increasing"


def test_timecodes_consistent_across_channels():
    log = ibt(TEST_FILE)
    ref_tc = log.channels["Speed"].column("timecodes").to_pylist()
    for name, table in log.channels.items():
        tc = table.column("timecodes").to_pylist()
        assert tc == ref_tc, f"Channel {name} timecodes differ from Speed"


def test_speed_range():
    log = ibt(TEST_FILE)
    speed = log.channels["Speed"].column("Speed").to_pylist()
    assert min(speed) >= 0.0, "Speed should be non-negative"
    assert max(speed) <= 200.0, "Speed (m/s) should be reasonable"


def test_lap_range():
    log = ibt(TEST_FILE)
    lap_vals = log.channels["Lap"].column("Lap").to_pylist()
    assert min(lap_vals) == 0
    assert max(lap_vals) == 11


def test_speed_metadata():
    log = ibt(TEST_FILE)
    field = log.channels["Speed"].schema.field("Speed")
    assert field.metadata is not None
    assert field.metadata[b"units"] == b"m/s"
    assert field.metadata[b"interpolate"] == b"True"
    assert b"desc" in field.metadata


def test_lap_metadata():
    log = ibt(TEST_FILE)
    field = log.channels["Lap"].schema.field("Lap")
    assert field.metadata is not None
    assert field.metadata[b"interpolate"] == b"False"


def test_session_time_metadata():
    log = ibt(TEST_FILE)
    field = log.channels["SessionTime"].schema.field("SessionTime")
    assert field.metadata is not None
    assert field.metadata[b"units"] == b"s"
    assert field.metadata[b"interpolate"] == b"True"


def test_gear_metadata():
    log = ibt(TEST_FILE)
    field = log.channels["Gear"].schema.field("Gear")
    assert field.metadata is not None
    assert field.metadata[b"interpolate"] == b"False"


def test_rpm_metadata():
    log = ibt(TEST_FILE)
    field = log.channels["RPM"].schema.field("RPM")
    assert field.metadata is not None
    assert field.metadata[b"units"] == b"revs/min"
    assert field.metadata[b"interpolate"] == b"True"
