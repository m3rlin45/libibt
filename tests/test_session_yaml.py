"""The Rust session-info YAML loader must agree with PyYAML.

``LogFile.metadata`` used to be completed in Python: ``yaml.safe_load`` parsed
``session_info_yaml`` and a block of ``dict.get`` calls derived the rest. That
now happens in the Rust core (``session_yaml.rs``, ``metadata.rs``), so every
binding gets the same document. These tests hold the Rust loader to PyYAML's
results: on the real fixtures, on a few thousand generated scalars (YAML 1.1
typing: booleans, base-60 numbers, dates, ...) and on a corpus of documents
(block and flow collections, quoting, anchors, merge keys, malformed input).

PyYAML is only a test dependency.
"""

import datetime
import random
from typing import Any

import pytest
import yaml

from libibt import ibt
from synthetic_ibt import VT_DOUBLE, VT_INT, build_ibt

FIXTURES = [
    "tests/test_data/test.ibt",
    "tests/test_data/formulair04_bathurst 2026-02-21 20-57-19.ibt",
]

VARIABLES = [
    ("SessionTime", VT_DOUBLE, 1, "s", "Seconds since session start"),
    ("Lap", VT_INT, 1, "", "Laps started count"),
]
RECORDS = [[0.0, 0], [1 / 60, 0]]

I64 = range(-(2**63), 2**63)


def typed(value: Any) -> Any:
    """A value with its types spelled out, so 1, 1.0 and True stay distinct,
    as do 0.0 and -0.0, and dict order counts."""
    if isinstance(value, dict):
        return ("dict", [(typed(k), typed(v)) for k, v in value.items()])
    if isinstance(value, list):
        return ("list", [typed(v) for v in value])
    if isinstance(value, float):
        return ("float", "nan" if value != value else value.hex())
    if isinstance(value, (datetime.date, datetime.datetime)):
        return (type(value).__name__, value.isoformat())
    return (type(value).__name__, value)


def rust_metadata(doc: str) -> dict[str, Any]:
    """``ibt().metadata`` of a minimal file whose session info is `doc`.

    The reader takes the session info from its first ``---``, so `doc`
    starts with one.
    """
    assert doc == "" or doc.startswith("---") or doc.startswith("%") or doc.startswith("\ufeff")
    return ibt(build_ibt(VARIABLES, RECORDS, session_yaml=doc)).metadata


def pyyaml_load(doc: str) -> tuple[str, Any]:
    try:
        return ("ok", yaml.safe_load(doc))
    except (yaml.YAMLError, ValueError, RecursionError) as e:
        return ("error", type(e).__name__)


def rust_load(doc: str) -> tuple[str, Any]:
    try:
        metadata = rust_metadata(doc)
    except ValueError as e:
        return ("error", str(e))
    # A falsy document (None, 0, "", [], {}) leaves no session_info entry.
    return ("ok", metadata["session_info"]) if "session_info" in metadata else ("falsy", None)


def assert_same_load(doc: str) -> None:
    expected, actual = pyyaml_load(doc), rust_load(doc)
    if expected[0] == "error":
        assert actual[0] == "error", f"{doc!r}: PyYAML raised {expected[1]}, Rust loaded {actual!r}"
    elif actual[0] == "falsy":
        assert not expected[1], f"{doc!r}: {expected!r}"
    else:
        assert actual[0] == "ok", f"{doc!r}: PyYAML loaded {expected[1]!r}, Rust raised {actual[1]}"
        assert typed(actual[1]) == typed(expected[1]), repr(doc)


# --- The derivation LogFile.__post_init__ used to do, kept as the reference ---


def reference_derived(parsed: Any) -> dict[str, Any]:
    """The keys the Python package derived from the parsed session info."""
    out: dict[str, Any] = {}
    if not parsed:
        return out
    out["session_info"] = parsed

    weekend = parsed.get("WeekendInfo", {})
    driver_info = parsed.get("DriverInfo", {})
    session_info = parsed.get("SessionInfo", {})

    out["event_type"] = weekend.get("EventType")
    current_num = session_info.get("CurrentSessionNum")
    sessions = session_info.get("Sessions", [])
    current_session = next((s for s in sessions if s.get("SessionNum") == current_num), None)
    if current_session:
        out["session_type"] = current_session.get("SessionType")
        out["session_name"] = current_session.get("SessionName")

    driver_idx = driver_info.get("DriverCarIdx")
    drivers = driver_info.get("Drivers", [])
    driver = next((d for d in drivers if d.get("CarIdx") == driver_idx), None)
    if driver:
        out["driver_name"] = driver.get("UserName")
        out["driver_user_id"] = driver.get("UserID")
        out["driver_irating"] = driver.get("IRating")
        out["driver_license"] = driver.get("LicString")
        out["car_name"] = driver.get("CarScreenName")
        out["car_id"] = driver.get("CarID")

    out["car_gear_count"] = driver_info.get("DriverCarGearNumForward")
    out["car_redline_rpm"] = driver_info.get("DriverCarRedLine")
    out["car_shift_rpm"] = driver_info.get("DriverCarSLShiftRPM")
    out["car_idle_rpm"] = driver_info.get("DriverCarIdleRPM")

    out["track_id"] = weekend.get("TrackID")
    out["track_type"] = weekend.get("TrackType")

    out["weather_temp"] = weekend.get("TrackAirTemp")
    out["weather_surface_temp"] = weekend.get("TrackSurfaceTemp")
    out["weather_humidity"] = weekend.get("TrackRelativeHumidity")
    out["weather_skies"] = weekend.get("TrackSkies")
    out["weather_wind_speed"] = weekend.get("TrackWindVel")
    out["weather_wind_dir"] = weekend.get("TrackWindDir")

    out["num_drivers"] = sum(1 for d in drivers if not d.get("CarIsPaceCar", 0))
    out["car_setup"] = parsed.get("CarSetup")

    split_info = parsed.get("SplitTimeInfo", {})
    out["sectors"] = split_info.get("Sectors")
    return out


def assert_derived_like_reference(metadata: dict[str, Any]) -> None:
    """Everything after the header counters is what the reference derives
    from PyYAML's parse of the same YAML, in the same order."""
    keys = list(metadata)
    split = keys.index("end_time") + 1
    derived = {key: metadata[key] for key in keys[split:]}
    reference = reference_derived(yaml.safe_load(metadata["session_info_yaml"]))
    assert typed(derived) == typed(reference)


# --- Real files ---


@pytest.mark.parametrize("path", FIXTURES, ids=["test", "bathurst"])
def test_fixture_metadata_matches_pyyaml(path: str) -> None:
    metadata = ibt(path).metadata
    parsed = yaml.safe_load(metadata["session_info_yaml"])
    assert typed(metadata["session_info"]) == typed(parsed)
    assert_derived_like_reference(metadata)

    # The fixtures exercise the typing that matters in practice: iRacing
    # writes empty values, on/off flags and a session date.
    assert leaf_types(metadata["session_info"]) == {
        "NoneType",
        "bool",
        "int",
        "float",
        "str",
        "date",
    }
    assert isinstance(
        metadata["session_info"]["WeekendInfo"]["WeekendOptions"]["Date"], datetime.date
    )
    assert isinstance(metadata["driver_name"], str) and isinstance(metadata["car_id"], int)


def leaf_types(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set().union(*(leaf_types(v) for v in value.values()))
    if isinstance(value, list):
        return set().union(*(leaf_types(v) for v in value))
    return {type(value).__name__}


# --- Scalars ---

HAND_PICKED_SCALARS = """
~ 0 -0 +0 00 0_ 0__7 08 09 0o7 0b 0b1_0 0x 0xFf -0x10 0x1G 0b12 017 -017 0777 1:2 1:60 01:02 1:02:03
190:20:30 1. 1.0 .5 -.5 +.5 1.e+5 1.0e5 1.0E+5 1e5 1.5e+ 1:30.5 1:30. 1:60.5 0:30.5 0:30 ._ .5_ 1._ 1_. - + .
.. ...x 1.2.3 .inf -.inf +.INF .Inf .iNF .nan -.nan .NaN .NAN .Nan yes y Y n on oN TRUE tRUE null nULL None
2026-02-27 2026-2-27 2026-02-7 0000-01-01 2026-13-01 2026-02-30 2024-02-29 2023-02-29 9999-12-31 10000-01-01
2026-02-27T20:40:38 2026-02-27t20:40:38.5 2026-02-27T20:40:38Z 2026-02-27T20:40:38.1234567 2026-02-27T24:00:00
2026-02-27T23:60:00 2026-02-27T23:59:60 2026-02-27T20:40 2026-02-27T20:40:38+0530 2026-02-27T20:40:38-05:30
9223372036854775807 -9223372036854775808 1_000_000 0.1 123456789.123456789 3.14159_26535 1__2.3__4 a:b a-b -a
1,5 0. -1. +1.5 1.5E+10 1.5e-10 .5e+2 .5e2 5.e+2 08.5 007.5 1e400
""".split() + [
    "2026-02-27 20:40:38",
    "2026-2-7 8:05:09",
    "2026-02-27 20:40:38.",
    "2026-02-27 20:40:38 Z",
    "2026-02-27 20:40:38 +5",
    "2026-02-27 20:40:38 -05:30",
    "2026-02-27 20:40:38 +24:00",
    "2026-02-27 20:40:38 +23:59",
    "2026-02-27 20:40:38 -0:00",
    "2026-02-27   20:40:38",
    "4.4069 km",
    "20.42 C",
    "71 %",
    "B 1.52",
    "8.91 m/s",
    "x y z",
    "1 2",
    "9223372036854775808",
    "-9223372036854775809",
    "1:2:3:4:5:6:7:8:9:10:11:12",
    "1." + "0" * 400,
    "9" * 400 + ".",
]

SCALAR_PIECES = """
0 1 7 8 9 12 59 60 007 1_0 _ - + . : e E e+ e- x b o 0x 0b 0o inf Inf INF nan NaN NAN .inf .nan ~ null Null
NULL yes Yes no No NO on On ON off Off true True TRUE false False y n T t Z 2026 -02 -2 -27 :05 :5 << = a km F f A
""".split() + [
    " "
]


def generated_scalars(count: int) -> list[str]:
    """Plain scalars built from the fragments PyYAML's resolvers care about."""
    rng = random.Random(1234)
    scalars = set(HAND_PICKED_SCALARS)
    while len(scalars) < count:
        text = "".join(rng.choice(SCALAR_PIECES) for _ in range(rng.choice([1, 2, 2, 3, 3, 4, 5])))
        text = text.strip()
        # Keep to what is one plain scalar as a block sequence entry.
        if not text or ": " in text or " #" in text or text.endswith(":"):
            continue
        if text[0] in "[]{},#&*!|>'\"%@`" or (text[0] in "-?:" and text[1:2] in ("", " ")):
            continue
        scalars.add(text)
    return sorted(scalars)


def test_plain_scalars_resolve_like_pyyaml() -> None:
    """Each scalar gets PyYAML's type and value, in one document; the ones
    PyYAML rejects (an impossible date, ``0b_``, a stray ``=``) are rejected."""
    loadable: list[tuple[str, Any]] = []
    rejected: list[str] = []
    beyond_i64: list[str] = []
    for text in generated_scalars(4000):
        status, value = pyyaml_load(f"---\n- {text}\n")
        if status == "error":
            rejected.append(text)
        elif type(value[0]) is int and value[0] not in I64:
            beyond_i64.append(text)
        else:
            loadable.append((text, value[0]))

    assert len(loadable) > 3500 and len(rejected) >= 8 and len(beyond_i64) >= 3

    doc = "---\n" + "".join(f"- {text}\n" for text, _ in loadable)
    actual = rust_metadata(doc)["session_info"]
    assert len(actual) == len(loadable)
    for (text, expected), value in zip(loadable, actual):
        assert typed(value) == typed(expected), repr(text)

    # Every scalar type is in the mix (a few entries, like "- x", nest a list).
    kinds = {type(value).__name__ for _, value in loadable}
    assert kinds >= {"NoneType", "bool", "int", "float", "str", "date", "datetime"}

    for text in rejected:
        with pytest.raises(ValueError):
            rust_metadata(f"---\n- {text}\n")
    # A documented difference: Python integers are unbounded, ours are i64.
    for text in beyond_i64:
        with pytest.raises(ValueError, match="out of range"):
            rust_metadata(f"---\n- {text}\n")


@pytest.mark.parametrize("template", ["---\nkey: {}\n", "---\n{}: value\n", "---\n[{}, x]\n"])
def test_scalars_resolve_the_same_as_values_keys_and_flow_entries(template: str) -> None:
    for text in generated_scalars(4000)[::9]:
        if template.startswith("---\n[") and text.startswith(":"):
            continue  # see test_documented_differences
        status, value = pyyaml_load(f"---\n- {text}\n")
        if status == "ok" and type(value[0]) is int and value[0] not in I64:
            continue
        assert_same_load(template.format(text))


# --- Documents ---

DOCUMENTS = {
    "block map": "---\na: 1\nb:\n  c: x\n  d: [1, 2]\n",
    "block seq of maps": "---\n- a: 1\n  b: 2\n- a: 3\n",
    "seq at the indent of its key": "---\nkey:\n- 1\n- 2\nother: x\n",
    "iracing layout": (
        "---\nWeekendInfo:\n TrackName: spa\n WeekendOptions:\n  Date: 2026-02-27\n"
        "  CourseCautions: off\nDrivers:\n - CarIdx: 0\n   UserName: A B\n"
    ),
    "quoted": "---\na: \"x: y\"\nb: 'it''s'\nc: \"tab\\there\\n\\u00e9\\x41\\/\"\nd: ''\ne: \"\"\n",
    "double-quoted escapes": '---\na: "\\0\\a\\b\\e\\f\\r\\v\\N\\_\\L\\P\\\\\\"\\ "\n',
    "quoted multiline": "---\na: \"line one\n  line two\n\n  line four\"\nb: 'single\n  folded'\n",
    "plain multiline": "---\na: word one\n  word two\n  word three\nb: 2\n",
    "literal": "---\na: |\n  line1\n  line2\nb: |-\n  strip\nc: |+\n  keep\n\nd: |2\n    indented\n",
    "folded": "---\na: >\n  folded\n  text\n\n  para\nb: >-\n  x\n  y\n",
    "comments": "---\n# top\na: 1 # trailing\nb: x#notcomment\nc: 'x # y'\n",
    "flow": "---\na: {x: 1, y: [1, 2, {z: no}]}\nb: []\nc: {}\nd: [a, b,]\n",
    "flow multi-line": "---\na: [1,\n  2,\n  3]\nb: {x: 1,\n  y: 2}\n",
    "anchors": "---\na: &x [1, 2]\nb: *x\nc: &y hello\nd: *y\n",
    "merge": "---\nbase: &b {x: 1, y: 2}\nm:\n  <<: *b\n  y: 3\n  z: 4\n",
    "merge list": "---\na: &a {k: 1, j: 9}\nb: &b {k: 2, l: 3}\nm:\n  <<: [*a, *b]\n  own: 5\n",
    "merge twice": "---\na: &a {k: 1}\nb: &b {k: 2, l: 3}\nm:\n  own: 5\n  <<: *a\n  <<: *b\n  k: 7\n",
    "repeated keys": "---\na: 1\nb: 2\na: 3\n",
    "typed keys": '---\n1: a\n1.0: b\nyes: c\n"1": d\n~: e\n2026-02-27: f\n',
    "null values": "---\na:\nb: ~\nc: null\nd:\n  e:\n",
    "empty seq entries": "---\n- \n- a\n-\n",
    "nested seq": "---\n- - a\n  - b\n- - c\n",
    "explicit keys": "---\n? a\n: 1\n? b\n",
    "unhashable key": "---\n[1, 2]: a\n",
    "two documents": "---\na: 1\n---\nb: 2\n",
    "document end marker": "---\na: 1\n...\n",
    "no content": "---\n",
    "only a comment": "--- # nothing\n",
    "scalar document": "--- hello\n",
    "integer document": "--- 12\n",
    "zero document": "--- 0\n",
    "list document": "---\n- 1\n- 2\n",
    "tab indent": "---\na:\n\tb: 1\n",
    "tab after colon": "---\na:\t1\n",
    "bad indent": "---\na: 1\n b: 2\n",
    "mapping in a value": "---\na: b: c\n",
    "colons without spaces": "---\na: b:c\nd: http://x.y/z\n",
    "at sign": "---\na: @home\n",
    "backtick": "---\na: `x`\n",
    "undefined alias": "---\na: *nope\n",
    "anchor without a node": "---\na: &anch\n",
    "percent": "---\na: 100%\nb: %x\n",
    "unterminated quote": '---\na: "abc\n',
    "unterminated flow": "---\na: [1, 2\n",
    "indicator-like values": "---\na: ?x\nb: x?\nc: -x\nd: - x\n",
    "dash values": "---\na: -\nb: --\n",
    "windows newlines": "---\r\na: 1\r\nb:\r\n  c: 2\r\n",
    "trailing spaces": "---\na: 1   \nb: x   \n",
    "unicode": "---\na: caf\u00e9 \u65e5\u672c \ufffd\nb: \u00e9: x\n",
    "control character": "---\na: x\x07y\n",
    "delete character": "---\na: x\x7fy\n",
    "byte order mark": "\ufeff---\na: 1\n",
    "yaml directive": "%YAML 1.1\n---\na: 1\n",
    "yaml 1.2 directive": "%YAML 1.2\n---\na: yes\n",
    "tag directive": "%TAG !e! tag:example.com,2000:\n---\na: 1\n",
    "long key": "---\n" + "k" * 2000 + ": 1\n",
    "implicit key over 1024 characters": "---\n" + "k" * 1030 + " : 1\n",
    "driver names": (
        '---\nDrivers:\n - UserName: O\'Brien Jr.\n - UserName: "Quoted Name"\n'
        " - UserName: Name, With Comma\n - UserName: Name #1\n - UserName: 'a: b'\n"
        " - UserName: null\n - UserName: 007\n - UserName: 1e5\n - UserName: Yes\n"
    ),
    "name with brackets": "---\na: [Team] Name\n",
    "name starting a flow mapping": "---\na: {x\n",
    "name ending in a colon": "---\na: Name:\n",
    "empty": "",
    "whitespace only": "---\n   \n",
    "value key": "---\n=: 1\n",
    "value as a value": "---\na: =\n",
    "merge as a value": "---\na: <<\n",
    "merge of a scalar": "---\n<<: 1\n",
    "set": "---\n? a\n? b\n",
    "indentless seq in seq": "---\na:\n- b:\n  - c\n",
    "keys with spaces": "---\nkey with spaces: 1\n? |\n  block key\n: 2\n",
    "single-pair flow entries": "---\n[a: 1, b: 2]\n",
    "flow implicit nulls": "---\n{a, b: }\n",
    "json": '---\n{"a": [1, 2.5, true, null, "x"]}\n',
    "json adjacent colon": '---\n{"a":1}\n',
    "block entry in flow": "---\n[- a]\n",
    "clock-like values": "---\na: 01:30\nb: 1:30\nc: 12:30:45\nd: 1:30 pm\ne: 2:10.123\n",
    "nested 100 deep": "---\n"
    + "".join(" " * i + "a:\n" for i in range(100))
    + " " * 100
    + "x: 1\n",
}


@pytest.mark.parametrize("name", DOCUMENTS)
def test_documents_load_like_pyyaml(name: str) -> None:
    assert_same_load(DOCUMENTS[name])


def test_documented_differences() -> None:
    """Where the Rust loader knowingly departs from PyYAML. None of these
    occurs in what iRacing writes."""
    # Explicit tags are not supported.
    assert yaml.safe_load("---\na: !!str 1\n") == {"a": "1"}
    with pytest.raises(ValueError, match="tags are not supported"):
        rust_metadata("---\na: !!str 1\n")

    # YAML 1.1 treats NEL (U+0085) as a line break; the YAML 1.2 scanner does not.
    assert yaml.safe_load("---\na: x\x85b: y\n") == {"a": "x", "b": "y"}
    with pytest.raises(ValueError):
        rust_metadata("---\na: x\x85b: y\n")

    # Nesting is capped (PyYAML goes as deep as Python's stack allows).
    deep = "---\n" + "".join(" " * i + "a:\n" for i in range(200)) + " " * 200 + "x: 1\n"
    assert yaml.safe_load(deep)
    with pytest.raises(ValueError, match="nested too deeply"):
        rust_metadata(deep)

    # A structure that contains itself cannot be built.
    recursive = yaml.safe_load("---\na: &a [*a]\n")
    assert recursive["a"][0] is recursive["a"]
    with pytest.raises(ValueError, match="not complete yet"):
        rust_metadata("---\na: &a [*a]\n")

    # In a flow collection PyYAML rejects a plain scalar that starts with a
    # colon; the YAML 1.2 scanner reads it as a string.
    with pytest.raises(yaml.YAMLError):
        yaml.safe_load("---\n[:05, x]\n")
    assert rust_metadata("---\n[:05, x]\n")["session_info"] == [":05", "x"]


# --- Derived entries ---

SESSION_DOC = """---
WeekendInfo:
 TrackName: spa
 TrackID: 152
 TrackType: road course
 EventType: Race
 TrackAirTemp: 20.42 C
 TrackSkies: Dynamic
SessionInfo:
 CurrentSessionNum: {current}
 Sessions:
 - SessionNum: 0
   SessionType: Practice
   SessionName: PRACTICE
 - SessionNum: 1
   SessionType: Race
   SessionName: RACE
DriverInfo:
 DriverCarIdx: {driver}
 DriverCarGearNumForward: 6
 DriverCarRedLine: 7300.000
 Drivers:
 - CarIdx: 0
   UserName: Pace Car
   CarIsPaceCar: 1
 - CarIdx: 7
   UserName: Driver Seven
   UserID: 77
   IRating: 1884
   LicString: B 1.52
   CarScreenName: FIA F4
   CarID: 148
   CarIsPaceCar: 0
 - CarIdx: 9
   UserName: 007
CarSetup:
 UpdateCount: 6
 Tires:
  LeftFront:
   StartingPressure: 150 kPa
SplitTimeInfo:
 Sectors:
 - SectorNum: 0
   SectorStartPct: 0.000000
 - SectorNum: 1
   SectorStartPct: 0.345678
"""

DERIVATION_DOCUMENTS = {
    "complete": SESSION_DOC.format(current=1, driver=7),
    "first session, pace car recording": SESSION_DOC.format(current=0, driver=0),
    "numbers compare by value": SESSION_DOC.format(current="1.0", driver="7.0"),
    "booleans compare as numbers": SESSION_DOC.format(current="yes", driver="no"),
    "no such session or driver": SESSION_DOC.format(current=5, driver=99),
    "string never equals a number": SESSION_DOC.format(current='"1"', driver='"7"'),
    "missing numbers match records without one": (
        "---\nSessionInfo:\n Sessions:\n - SessionType: Race\n   SessionName: RACE\n"
        "DriverInfo:\n Drivers:\n - UserName: Solo\n"
    ),
    "weekend only": "---\nWeekendInfo:\n EventType: Test\n TrackWindVel: 8.91 m/s\n",
    "empty sections": "---\nWeekendInfo: {}\nDriverInfo: {}\nSessionInfo: {}\nSplitTimeInfo: {}\n",
    "no sections": "---\nSomethingElse: 1\n",
    "empty records": "---\nSessionInfo:\n Sessions: [{}]\nDriverInfo:\n Drivers: [{}, {}]\n",
    "pace car flags": (
        "---\nDriverInfo:\n Drivers:\n - CarIsPaceCar: 0\n - CarIsPaceCar: 1\n - CarIsPaceCar: no\n"
        " - CarIsPaceCar: yes\n - CarIsPaceCar: ''\n - CarIsPaceCar: '0'\n - CarIsPaceCar:\n - {}\n"
    ),
    "falsy document": "--- 0\n",
    "empty document": "---\n",
}


@pytest.mark.parametrize("name", DERIVATION_DOCUMENTS)
def test_derived_entries_match_the_python_reference(name: str) -> None:
    metadata = rust_metadata(DERIVATION_DOCUMENTS[name])
    assert_derived_like_reference(metadata)


def test_derived_entries_of_a_complete_document() -> None:
    metadata = rust_metadata(DERIVATION_DOCUMENTS["complete"])
    assert metadata["session_name"] == "RACE"
    assert metadata["driver_name"] == "Driver Seven"
    assert metadata["car_redline_rpm"] == 7300.0
    assert metadata["num_drivers"] == 2
    assert metadata["car_setup"]["Tires"]["LeftFront"]["StartingPressure"] == "150 kPa"
    assert [s["SectorNum"] for s in metadata["sectors"]] == [0, 1]
    # `007` is YAML 1.1 octal, as PyYAML reads it.
    assert metadata["session_info"]["DriverInfo"]["Drivers"][2]["UserName"] == 7


def test_misshapen_sections_count_as_empty() -> None:
    """The Python code raised AttributeError when a section was not the
    mapping or list it expected; the Rust core treats it as missing."""
    metadata = rust_metadata(
        "---\nWeekendInfo: nothing\nDriverInfo:\n Drivers: none\nSessionInfo: [1, 2]\n"
    )
    assert metadata["session_info"]["WeekendInfo"] == "nothing"
    assert metadata["event_type"] is None
    assert "session_type" not in metadata and "driver_name" not in metadata
    assert metadata["num_drivers"] == 0
    assert metadata["car_setup"] is None and metadata["sectors"] is None

    # A document that is not a mapping at all still loads.
    assert rust_metadata("---\n- 1\n- 2\n")["session_info"] == [1, 2]


def test_malformed_session_yaml_is_a_value_error() -> None:
    with pytest.raises(ValueError, match="YAML error"):
        rust_metadata("---\na: [1, 2\n")


def test_no_session_info_means_no_derived_entries() -> None:
    metadata = rust_metadata("")
    assert metadata["session_info_yaml"] == ""
    assert list(metadata)[-1] == "end_time"
