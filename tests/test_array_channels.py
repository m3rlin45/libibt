"""Tests for array variable (count > 1) channel expansion."""

import struct

import pyarrow as pa
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
def array_vars(raw_data):
    arrays = [vh for vh in read_var_headers(raw_data) if vh[3] > 1]
    assert arrays, "test file should contain at least one array variable"
    return arrays


def test_array_elements_become_channels(log, array_vars):
    for name, _var_type, _offset, count in array_vars:
        for i in range(count):
            assert f"{name}[{i}]" in log.channels, f"Missing element channel {name}[{i}]"
        assert name not in log.channels
        assert f"{name}[{count}]" not in log.channels


def test_array_element_channel_shape(log, array_vars):
    name, _var_type, _offset, count = array_vars[0]
    scalar_rows = len(next(iter(log.channels.values())))
    for i in range(count):
        table = log.channels[f"{name}[{i}]"]
        assert table.column_names == ["timecodes", f"{name}[{i}]"]
        assert len(table) == scalar_rows


def test_array_element_metadata(log, array_vars):
    name, _var_type, _offset, count = array_vars[0]
    for i in range(count):
        meta = ChannelMetadata.from_channel_table(log.channels[f"{name}[{i}]"])
        assert meta.units != "" or meta.desc != ""
        # float array elements should interpolate
        assert meta.interpolate is True


def test_array_element_values_match_raw(log, raw_data, array_vars):
    """Element values must match a direct struct read of the raw records."""
    buf_len = struct.unpack_from("<i", raw_data, 36)[0]
    buf_offset = struct.unpack_from("<i", raw_data, 52)[0]  # varBuf[0].bufOffset

    fmt_by_type = {0: "B", 1: "B", 2: "i", 3: "I", 4: "f", 5: "d"}
    size_by_type = {0: 1, 1: 1, 2: 4, 3: 4, 4: 4, 5: 8}

    for name, var_type, offset, count in array_vars:
        fmt = "<" + fmt_by_type[var_type]
        elem_size = size_by_type[var_type]
        for i in (0, count - 1):
            table = log.channels[f"{name}[{i}]"]
            values = table.column(f"{name}[{i}]").to_pylist()
            for record in (0, 1, len(values) - 1):
                raw_off = buf_offset + record * buf_len + offset + i * elem_size
                expected = struct.unpack_from(fmt, raw_data, raw_off)[0]
                assert values[record] == pytest.approx(expected), (
                    f"{name}[{i}] record {record}: got {values[record]}, " f"raw value {expected}"
                )


def test_array_channels_work_with_logfile_methods(log, array_vars):
    name, _var_type, _offset, count = array_vars[0]
    element = f"{name}[0]"

    subset = log.select_channels([element])
    assert list(subset.channels.keys()) == [element]

    lap_nums = log.laps.column("num").to_pylist()
    filtered = log.filter_by_lap(lap_nums[1], channel_names=[element])
    assert len(filtered.channels[element]) < len(log.channels[element])

    merged = subset.get_channels_as_table()
    assert element in merged.column_names
