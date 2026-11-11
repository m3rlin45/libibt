"""Tests for time-subsample (`_ST`) array variables.

`_ST` arrays are merged into a single higher-rate channel. Entity-indexed
arrays (e.g. CarIdx*) become vector-valued FixedSizeList channels — see
test_list_channels.py.
"""

import struct

import numpy as np
import pytest

from libibt import ibt, ChannelMetadata

TEST_FILE = "tests/test_data/test.ibt"

VAR_HEADER_SIZE = 144


def read_var_headers(data: bytes):
    """Minimal reference parse of the IBT variable headers."""
    header = struct.unpack_from("<8i", data, 0)
    num_vars, var_header_offset = header[6], header[7]
    var_headers = []
    for i in range(num_vars):
        off = var_header_offset + i * VAR_HEADER_SIZE
        var_type, offset, count = struct.unpack_from("<3i", data, off)
        name = data[off + 16 : off + 48].split(b"\0")[0].decode()
        var_headers.append((name, var_type, offset, count))
    return var_headers


@pytest.fixture(scope="module")
def raw_data() -> bytes:
    with open(TEST_FILE, "rb") as f:
        return f.read()


@pytest.fixture(scope="module")
def log():
    return ibt(TEST_FILE)


@pytest.fixture(scope="module")
def st_vars(raw_data):
    st = [vh for vh in read_var_headers(raw_data) if vh[3] > 1 and vh[0].endswith("_ST")]
    assert st, "test file should contain at least one _ST array variable"
    return st


def test_st_arrays_become_merged_channels(log, st_vars):
    for name, _var_type, _offset, count in st_vars:
        assert name in log.channels, f"Missing merged channel {name}"
        assert f"{name}[0]" not in log.channels


def test_st_channel_shape(log, st_vars):
    scalar_rows = len(log.channels["Speed"])
    for name, _var_type, _offset, count in st_vars:
        table = log.channels[name]
        assert table.column_names == ["timecodes", name]
        assert len(table) == scalar_rows * count


def test_st_channel_timecodes(log, st_vars):
    """Sub-sample timecodes interleave between ticks; the last sub-sample of
    each tick lands exactly on the tick's timestamp."""
    name, _var_type, _offset, count = st_vars[0]
    sub_tc = log.channels[name].column("timecodes").to_numpy()
    tick_tc = log.channels["Speed"].column("timecodes").to_numpy()

    assert np.all(np.diff(sub_tc) > 0), "sub-sample timecodes must be strictly increasing"
    np.testing.assert_array_equal(sub_tc[count - 1 :: count], tick_tc)


def test_st_channel_matches_scalar_channel(log, st_vars):
    """The last sub-sample of each tick equals the scalar channel's value
    (e.g. SteeringWheelTorque_ST vs SteeringWheelTorque)."""
    for name, _var_type, _offset, count in st_vars:
        scalar_name = name.removesuffix("_ST")
        if scalar_name not in log.channels:
            continue
        sub_values = log.channels[name].column(name).to_numpy()
        scalar_values = log.channels[scalar_name].column(scalar_name).to_numpy()
        np.testing.assert_array_equal(sub_values[count - 1 :: count], scalar_values)


def test_st_channel_values_match_raw(log, raw_data, st_vars):
    """Interleaved values must match a direct struct read of the raw records."""
    buf_len = struct.unpack_from("<i", raw_data, 36)[0]
    buf_offset = struct.unpack_from("<i", raw_data, 52)[0]  # varBuf[0].bufOffset

    fmt_by_type = {0: "B", 1: "B", 2: "i", 3: "I", 4: "f", 5: "d"}
    size_by_type = {0: 1, 1: 1, 2: 4, 3: 4, 4: 4, 5: 8}

    for name, var_type, offset, count in st_vars:
        fmt = "<" + fmt_by_type[var_type]
        elem_size = size_by_type[var_type]
        values = log.channels[name].column(name).to_pylist()
        record_count = len(values) // count
        for record in (0, 1, record_count - 1):
            for k in (0, count - 1):
                raw_off = buf_offset + record * buf_len + offset + k * elem_size
                expected = struct.unpack_from(fmt, raw_data, raw_off)[0]
                actual = values[record * count + k]
                assert actual == pytest.approx(expected), (
                    f"{name} record {record} sub-sample {k}: got {actual}, " f"raw value {expected}"
                )


def test_st_channel_metadata(log, st_vars):
    for name, _var_type, _offset, _count in st_vars:
        meta = ChannelMetadata.from_channel_table(log.channels[name])
        assert meta.units != "" or meta.desc != ""
        # _ST variables are float sub-samples and should interpolate
        assert meta.interpolate is True


def test_st_channels_work_with_logfile_methods(log, st_vars):
    name, _var_type, _offset, count = st_vars[0]

    subset = log.select_channels([name])
    assert list(subset.channels.keys()) == [name]

    lap_nums = log.laps.column("num").to_pylist()
    filtered = log.filter_by_lap(lap_nums[1], channel_names=[name])
    assert 0 < len(filtered.channels[name]) < len(log.channels[name])

    # Resampling the high-rate channel down to the tick timebase keeps the
    # tick-aligned values
    resampled = log.resample_to_channel("Speed", channel_names=[name])
    assert len(resampled.channels[name]) == len(log.channels["Speed"])
