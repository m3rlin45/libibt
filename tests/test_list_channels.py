"""Tests for entity-indexed array variables (vector-valued channels).

Non-`_ST` array variables (e.g. per-car CarIdx arrays) become one channel
whose value column is a FixedSizeList — one vector per tick, mirroring how
pyirsdk and the official SDK expose array variables.

The real test file contains no such variables, so these tests use a
hand-crafted synthetic IBT file.
"""

import numpy as np
import pyarrow as pa
import pytest

from libibt import ibt, ChannelMetadata

from synthetic_ibt import build_ibt, VT_DOUBLE, VT_INT, VT_FLOAT

N_CARS = 4
N_RECORDS = 12

VARIABLES = [
    ("SessionTime", VT_DOUBLE, 1, "s", "Seconds since session start"),
    ("Lap", VT_INT, 1, "", "Laps started count"),
    ("Speed", VT_FLOAT, 1, "m/s", "GPS vehicle speed"),
    ("CarIdxLapDistPct", VT_FLOAT, N_CARS, "%", "Percentage distance around lap by car index"),
    ("CarIdxPosition", VT_INT, N_CARS, "", "Position in race by car index"),
]


def lap_dist_pct(record, car):
    return 0.1 * record + car


@pytest.fixture(scope="module")
def log():
    records = []
    for i in range(N_RECORDS):
        records.append(
            [
                i / 60.0,  # SessionTime
                0 if i < 6 else 1,  # Lap
                10.0 + i,  # Speed
                [lap_dist_pct(i, j) for j in range(N_CARS)],  # CarIdxLapDistPct
                [j + 1 for j in range(N_CARS)],  # CarIdxPosition
            ]
        )
    return ibt(build_ibt(VARIABLES, records))


def as_2d(table, name):
    col = table.column(name).combine_chunks()
    return col.values.to_numpy(zero_copy_only=False).reshape(-1, col.type.list_size)


def test_array_var_becomes_vector_channel(log):
    assert "CarIdxLapDistPct" in log.channels
    assert "CarIdxLapDistPct[0]" not in log.channels

    table = log.channels["CarIdxLapDistPct"]
    field = table.schema.field("CarIdxLapDistPct")
    assert pa.types.is_fixed_size_list(field.type)
    assert field.type.list_size == N_CARS
    assert field.type.value_type == pa.float32()
    assert len(table) == N_RECORDS

    int_field = log.channels["CarIdxPosition"].schema.field("CarIdxPosition")
    assert pa.types.is_fixed_size_list(int_field.type)
    assert int_field.type.value_type == pa.int32()


def test_vector_channel_values(log):
    values = as_2d(log.channels["CarIdxLapDistPct"], "CarIdxLapDistPct")
    expected = np.array(
        [[lap_dist_pct(i, j) for j in range(N_CARS)] for i in range(N_RECORDS)],
        dtype=np.float32,
    )
    np.testing.assert_array_equal(values, expected)

    positions = as_2d(log.channels["CarIdxPosition"], "CarIdxPosition")
    assert positions.shape == (N_RECORDS, N_CARS)
    np.testing.assert_array_equal(positions[0], [1, 2, 3, 4])


def test_vector_channel_metadata(log):
    meta = ChannelMetadata.from_channel_table(log.channels["CarIdxLapDistPct"])
    assert meta.units == "%"
    assert meta.interpolate is True

    meta = ChannelMetadata.from_channel_table(log.channels["CarIdxPosition"])
    assert meta.interpolate is False


def test_scalar_channels_unaffected(log):
    speed = log.channels["Speed"]
    assert speed.column("Speed").to_pylist() == [10.0 + i for i in range(N_RECORDS)]
    assert not pa.types.is_fixed_size_list(speed.schema.field("Speed").type)


def test_resample_interpolates_per_column(log):
    tc = log.channels["Speed"].column("timecodes").to_numpy()
    target = pa.array([int(tc[0] + 8), int(tc[4])], type=pa.int64())
    resampled = log.resample_to_timecodes(target)

    table = resampled.channels["CarIdxLapDistPct"]
    assert len(table) == 2
    values = as_2d(table, "CarIdxLapDistPct")

    source = as_2d(log.channels["CarIdxLapDistPct"], "CarIdxLapDistPct")
    for j in range(N_CARS):
        expected = np.interp(target.to_numpy(), tc, source[:, j].astype(np.float64))
        np.testing.assert_allclose(values[:, j], expected, rtol=1e-6)

    # metadata survives resampling
    meta = ChannelMetadata.from_channel_table(table)
    assert meta.units == "%"


def test_resample_forward_fills_int_vectors(log):
    tc = log.channels["Speed"].column("timecodes").to_numpy()
    # between ticks 0 and 1, and before the first tick
    target = pa.array([int(tc[0]) - 5, int(tc[0]) + 8], type=pa.int64())
    resampled = log.resample_to_timecodes(target)

    values = as_2d(resampled.channels["CarIdxPosition"], "CarIdxPosition")
    np.testing.assert_array_equal(values, [[1, 2, 3, 4], [1, 2, 3, 4]])
    # dtype preserved (no float promotion for non-interpolating channels)
    assert values.dtype == np.int32


def test_filter_by_time_range_on_vector_channel(log):
    tc = log.channels["Speed"].column("timecodes").to_numpy()
    filtered = log.filter_by_time_range(int(tc[2]), int(tc[5]))
    table = filtered.channels["CarIdxLapDistPct"]
    assert len(table) == 3
    values = as_2d(table, "CarIdxLapDistPct")
    np.testing.assert_allclose(values[0], [lap_dist_pct(2, j) for j in range(N_CARS)], rtol=1e-6)


def test_get_channels_as_table_includes_vector_channels(log):
    merged = log.get_channels_as_table()
    assert "CarIdxLapDistPct" in merged.column_names
    assert "Speed" in merged.column_names
    assert len(merged) == N_RECORDS

    field = merged.schema.field("CarIdxLapDistPct")
    assert pa.types.is_fixed_size_list(field.type)
    meta = ChannelMetadata.from_field(field)
    assert meta.units == "%"

    values = as_2d(merged, "CarIdxLapDistPct")
    np.testing.assert_allclose(values[-1][0], lap_dist_pct(N_RECORDS - 1, 0), rtol=1e-6)


def test_select_channels_with_vector_channel(log):
    subset = log.select_channels(["CarIdxLapDistPct", "Speed"])
    assert set(subset.channels.keys()) == {"CarIdxLapDistPct", "Speed"}


def test_laps_from_synthetic_file(log):
    nums = log.laps.column("num").to_pylist()
    assert nums == [0, 1]
