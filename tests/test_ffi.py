"""The C ABI (crates/libibt-ffi) must hand out what the Python LogFile holds.

Loads the ``ibt_ffi`` shared library with ctypes, imports every channel and
the laps table through the Arrow C Data Interface, and compares them with
``ibt()`` on every fixture: channel names and order, schemas including field
metadata, bit-exact timecodes and values, laps, and the metadata document
with its key order.

The library is not part of the Python package, so these tests only run when
``LIBIBT_FFI_LIB`` names it (absolute, or relative to the repository root).
``just ffi-test`` builds it and sets the variable.
"""

import ctypes
import datetime
import json
import math
import os
from pathlib import Path
from typing import Any, Callable, Iterator

import pyarrow as pa
import pytest

from libibt import ibt
from synthetic_ibt import VT_BOOL, VT_DOUBLE, VT_FLOAT, VT_INT, build_ibt

REPO_ROOT = Path(__file__).parent.parent
TEST_DATA_DIR = Path(__file__).parent / "test_data"
FIXTURES = sorted(TEST_DATA_DIR.glob("*.ibt"))
TEST_IBT = TEST_DATA_DIR / "test.ibt"

_LIB_ENV = os.environ.get("LIBIBT_FFI_LIB", "")

pytestmark = pytest.mark.skipif(
    not _LIB_ENV, reason="LIBIBT_FFI_LIB is not set (run `just ffi-test`)"
)

IBT_OK = 0
IBT_ERR_INDEX = 1


class Ffi:
    """The ten functions of the C ABI, with their signatures."""

    def __init__(self, path: Path) -> None:
        lib = ctypes.CDLL(str(path))
        session = ctypes.c_void_p
        string = ctypes.c_void_p  # owned char*: read with string_at, then free
        error = ctypes.POINTER(ctypes.c_void_p)

        def bind(name: str, restype: Any, *argtypes: Any) -> Any:
            fn = getattr(lib, name)
            fn.restype = restype
            fn.argtypes = list(argtypes)
            return fn

        self.open_path = bind("ibt_open_path", session, ctypes.c_char_p, error)
        self.open_bytes = bind("ibt_open_bytes", session, ctypes.c_char_p, ctypes.c_size_t, error)
        self.close = bind("ibt_close", None, session)
        self.channel_count = bind("ibt_channel_count", ctypes.c_size_t, session)
        self.channel_name = bind("ibt_channel_name", string, session, ctypes.c_size_t)
        self.metadata_json = bind("ibt_metadata_json", string, session)
        self.channel_stream = bind(
            "ibt_channel_stream", ctypes.c_int, session, ctypes.c_size_t, ctypes.c_void_p
        )
        self.laps_stream = bind("ibt_laps_stream", ctypes.c_int, session, ctypes.c_void_p)
        self.string_free = bind("ibt_string_free", None, string)
        self.version = bind("ibt_version", ctypes.c_char_p)

    def take_string(self, pointer: Any) -> str:
        assert pointer, "expected a string, got NULL"
        try:
            return ctypes.string_at(pointer).decode("utf-8")
        finally:
            self.string_free(pointer)

    def open(self, source: "Path | bytes") -> int:
        error = ctypes.c_void_p()
        if isinstance(source, bytes):
            handle = self.open_bytes(source, len(source), ctypes.byref(error))
        else:
            handle = self.open_path(str(source).encode("utf-8"), ctypes.byref(error))
        if not handle:
            raise ValueError(self.take_string(error.value))
        assert not error.value
        return int(handle)

    def table(self, export: Callable[..., int], *args: Any) -> pa.Table:
        # struct ArrowArrayStream is five pointers; the reader takes it over.
        stream = (ctypes.c_void_p * 5)()
        code = export(*args, ctypes.addressof(stream))
        assert code == IBT_OK, f"export failed with code {code}"
        reader = pa.RecordBatchReader._import_from_c(ctypes.addressof(stream))
        return reader.read_all()


@pytest.fixture(scope="module")
def ffi() -> Ffi:
    path = Path(_LIB_ENV)
    if not path.is_absolute():
        path = REPO_ROOT / path
    assert path.is_file(), f"LIBIBT_FFI_LIB={_LIB_ENV!r}: {path} does not exist"
    return Ffi(path)


@pytest.fixture
def opened(ffi: Ffi) -> Iterator[Callable[["Path | bytes"], int]]:
    """Open sessions that are closed again when the test ends."""
    handles: list[int] = []

    def _open(source: "Path | bytes") -> int:
        handles.append(ffi.open(source))
        return handles[-1]

    yield _open
    for handle in handles:
        ffi.close(handle)


def _raw(column: pa.ChunkedArray) -> Any:
    """A column's values in a form that compares bit for bit."""
    array = column.combine_chunks()
    # An array variable is a FixedSizeList column: compare its flat values
    # (the list size is part of the schema, which is compared separately).
    while pa.types.is_fixed_size_list(array.type):
        array = array.flatten()
    if pa.types.is_string(array.type):
        return array.to_pylist()
    # Bytes, not values: NaN payloads and -0.0 must survive too.
    values = array.to_numpy(zero_copy_only=False)
    return (str(values.dtype), values.tobytes())


def _assert_same_table(actual: pa.Table, expected: pa.Table, what: str) -> None:
    assert actual.schema.equals(expected.schema, check_metadata=True), (
        f"{what}: schema {actual.schema.to_string(show_field_metadata=True)!r} != "
        f"{expected.schema.to_string(show_field_metadata=True)!r}"
    )
    assert actual.num_rows == expected.num_rows, what
    for name in expected.column_names:
        assert _raw(actual.column(name)) == _raw(expected.column(name)), f"{what}: {name} differs"


def _json_view(value: Any) -> Any:
    """A metadata value as its JSON round trip yields it, keeping dict order."""
    if isinstance(value, dict):
        return [(_json_key(key), _json_view(item)) for key, item in value.items()]
    if isinstance(value, list):
        return [_json_view(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return ("NoneType", None)  # JSON has no NaN or infinity; the ABI writes null
    if isinstance(value, (datetime.date, datetime.datetime)):
        return ("str", value.isoformat())  # no JSON type either: ISO 8601
    return (type(value).__name__, value)


def _json_key(key: Any) -> str:
    """A mapping key as the ABI writes it: JSON keys are strings."""
    if isinstance(key, str):
        return key
    if isinstance(key, (datetime.date, datetime.datetime)):
        return key.isoformat()
    return json.dumps(key)


# Session info with what JSON cannot say directly: dates and times, NaN and
# infinity, and mapping keys that are not strings.
SESSION_YAML = """---
WeekendInfo:
 TrackName: synthetic
 WeekendOptions:
  Date: 2026-02-27
  Start: 2026-02-27 20:40:38
  StartUtc: 2026-02-27T20:40:38.5Z
  StartLocal: 2026-02-27 20:40:38.000123 -05:30
  Wind: .nan
  Limit: .inf
  CourseCautions: off
  Empty:
Lookup:
 1: one
 2.5: two and a half
 no: flag
 ~: nothing
 2026-02-27: day
"""


def _assert_session_matches(ffi: Ffi, session: int, expected: Any) -> None:
    """The session holds what the LogFile `expected` holds."""
    count = ffi.channel_count(session)
    names = [ffi.take_string(ffi.channel_name(session, i)) for i in range(count)]
    assert names == list(expected.channels)

    for index, name in enumerate(names):
        table = ffi.table(ffi.channel_stream, session, index)
        _assert_same_table(table, expected.channels[name], name)

    _assert_same_table(ffi.table(ffi.laps_stream, session), expected.laps, "laps")

    document = json.loads(ffi.take_string(ffi.metadata_json(session)))
    assert _json_view(document) == _json_view(expected.metadata)


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda p: p.name)
def test_session_matches_python_logfile(
    ffi: Ffi, opened: Callable[["Path | bytes"], int], fixture: Path
) -> None:
    _assert_session_matches(ffi, opened(fixture), ibt(str(fixture)))


def test_array_variables_match_python_logfile(
    ffi: Ffi, opened: Callable[["Path | bytes"], int]
) -> None:
    """The fixtures carry no entity-indexed arrays (CarIdx*), which come out
    as FixedSizeList columns, so that path runs on a synthetic file."""
    variables = [
        ("SessionTime", VT_DOUBLE, 1, "s", "Seconds since session start"),
        ("Lap", VT_INT, 1, "", "Laps started count"),
        ("CarIdxLapDistPct", VT_FLOAT, 3, "%", "Percentage distance around lap by car index"),
        ("CarIdxLap", VT_INT, 3, "", "Laps started by car index"),
        ("CarIdxOnPitRoad", VT_BOOL, 3, "", "On pit road between the cones by car index"),
        ("Speed", VT_FLOAT, 1, "m/s", "GPS vehicle speed"),
    ]
    records = [
        [i / 60.0, i // 4, [i * 0.1, math.nan, -0.0], [i, -1, i * 2], [i % 2, 0, 1], i * 1.5]
        for i in range(12)
    ]
    data = build_ibt(variables, records, session_yaml=SESSION_YAML)

    expected = ibt(data)
    lists = [
        name
        for name, table in expected.channels.items()
        if pa.types.is_fixed_size_list(table.schema.field(name).type)
    ]
    assert lists == ["CarIdxLapDistPct", "CarIdxLap", "CarIdxOnPitRoad"]
    session = opened(data)
    _assert_session_matches(ffi, session, expected)

    # Spelled out once: how the values JSON has no type for are written.
    info = json.loads(ffi.take_string(ffi.metadata_json(session)))["session_info"]
    assert info["WeekendInfo"]["WeekendOptions"] == {
        "Date": "2026-02-27",
        "Start": "2026-02-27T20:40:38",
        "StartUtc": "2026-02-27T20:40:38.500000+00:00",
        "StartLocal": "2026-02-27T20:40:38.000123-05:30",
        "Wind": None,
        "Limit": None,
        "CourseCautions": False,
        "Empty": None,
    }
    assert info["Lookup"] == {
        "1": "one",
        "2.5": "two and a half",
        "false": "flag",
        "null": "nothing",
        "2026-02-27": "day",
    }


def test_open_bytes_matches_open_path(ffi: Ffi, opened: Callable[["Path | bytes"], int]) -> None:
    from_path = opened(TEST_IBT)
    from_bytes = opened(TEST_IBT.read_bytes())

    count = ffi.channel_count(from_path)
    assert count > 0 and ffi.channel_count(from_bytes) == count
    for index in range(count):
        _assert_same_table(
            ffi.table(ffi.channel_stream, from_bytes, index),
            ffi.table(ffi.channel_stream, from_path, index),
            f"channel {index}",
        )
    _assert_same_table(
        ffi.table(ffi.laps_stream, from_bytes), ffi.table(ffi.laps_stream, from_path), "laps"
    )
    assert ffi.take_string(ffi.metadata_json(from_bytes)) == ffi.take_string(
        ffi.metadata_json(from_path)
    )


def test_exported_tables_outlive_the_session(ffi: Ffi) -> None:
    expected = ibt(str(TEST_IBT))
    session = ffi.open(TEST_IBT)
    name = ffi.take_string(ffi.channel_name(session, 0))
    table = ffi.table(ffi.channel_stream, session, 0)
    ffi.close(session)
    _assert_same_table(table, expected.channels[name], name)


def test_errors_come_back_as_strings(ffi: Ffi, opened: Callable[["Path | bytes"], int]) -> None:
    missing = TEST_DATA_DIR / "no such file.ibt"
    with pytest.raises(ValueError, match="no such file.ibt"):
        ffi.open(missing)
    with pytest.raises(ValueError):
        ffi.open(b"not an ibt file")

    session = opened(TEST_IBT)
    count = ffi.channel_count(session)
    assert not ffi.channel_name(session, count)
    stream = (ctypes.c_void_p * 5)()
    assert ffi.channel_stream(session, count, ctypes.addressof(stream)) == IBT_ERR_INDEX
    assert ffi.version().decode("ascii")
