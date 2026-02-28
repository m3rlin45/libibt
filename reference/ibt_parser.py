#!/usr/bin/env python3
"""
Reference IBT telemetry file parser using the `construct` library.

This is a ground-truth debug/reference tool for validating the Rust implementation.
NOT production code.

Usage:
    uv run --no-project --with construct --with pyyaml python3 reference/ibt_parser.py [path]

If no path is given, defaults to tests/test_data/test.ibt (relative to this script).
"""

from __future__ import annotations

import json
import struct
import sys
from pathlib import Path
from typing import Any

import construct as cs
import yaml

# ---------------------------------------------------------------------------
# VarType enum (matches irsdk)
# ---------------------------------------------------------------------------

VAR_TYPE_NAMES = {
    0: "char",
    1: "bool",
    2: "int",
    3: "bitfield",
    4: "float",
    5: "double",
}

VAR_TYPE_SIZES = {
    0: 1,  # char
    1: 1,  # bool
    2: 4,  # int
    3: 4,  # bitfield
    4: 4,  # float
    5: 8,  # double
}

VAR_TYPE_STRUCT_FMT = {
    0: "c",  # char -> bytes
    1: "?",  # bool
    2: "i",  # int32
    3: "I",  # uint32 (bitfield)
    4: "f",  # float32
    5: "d",  # float64
}

# ---------------------------------------------------------------------------
# Construct definitions for the binary structures
# ---------------------------------------------------------------------------

VarBuf = cs.Struct(
    "tickCount" / cs.Int32sl,
    "bufOffset" / cs.Int32sl,
    "pad" / cs.Bytes(8),
)

IrsdkHeader = cs.Struct(
    "ver" / cs.Int32sl,
    "status" / cs.Int32sl,
    "tickRate" / cs.Int32sl,
    "sessionInfoUpdate" / cs.Int32sl,
    "sessionInfoOffset" / cs.Int32sl,
    "sessionInfoLen" / cs.Int32sl,
    "numVars" / cs.Int32sl,
    "varHeaderOffset" / cs.Int32sl,
    "numBuf" / cs.Int32sl,
    "bufLen" / cs.Int32sl,
    "pad" / cs.Bytes(8),  # 2x int32
    "varBuf" / cs.Array(4, VarBuf),
)

DiskSubHeader = cs.Struct(
    "sessionStartDate" / cs.Int64sl,
    "startTime" / cs.Float64l,
    "endTime" / cs.Float64l,
    "lapCount" / cs.Int32sl,
    "sessionRecordCount" / cs.Int32sl,
)

VarHeader = cs.Struct(
    "type" / cs.Int32sl,
    "offset" / cs.Int32sl,
    "count" / cs.Int32sl,
    "countAsTime" / cs.Int8sl,
    "pad" / cs.Bytes(3),
    "name" / cs.PaddedString(32, "ascii"),
    "desc" / cs.PaddedString(64, "ascii"),
    "unit" / cs.PaddedString(32, "ascii"),
)

# ---------------------------------------------------------------------------
# Parser functions
# ---------------------------------------------------------------------------


def parse_ibt(path: str | Path) -> dict[str, Any]:
    """
    Parse an IBT file and return a dict with all metadata plus an open
    file handle for reading records.

    Returns a dict with keys:
        header        - parsed irsdk_header
        disk_header   - parsed disk sub-header
        var_headers   - list of parsed variable headers
        var_lookup    - dict mapping variable name -> var header
        session_yaml  - session info as a parsed YAML object
        session_yaml_raw - raw YAML string
        path          - Path to the file
        _file         - the open file handle (caller should close)
    """
    path = Path(path)
    f = open(path, "rb")

    # --- Main header (112 bytes at offset 0) ---
    header = IrsdkHeader.parse_stream(f)

    # --- Disk sub-header (32 bytes at offset 112) ---
    disk_header = DiskSubHeader.parse_stream(f)

    # --- Variable headers ---
    f.seek(header.varHeaderOffset)
    var_headers = []
    for _ in range(header.numVars):
        vh = VarHeader.parse_stream(f)
        var_headers.append(vh)

    var_lookup = {vh.name: vh for vh in var_headers}

    # --- Session info YAML ---
    f.seek(header.sessionInfoOffset)
    raw_session = f.read(header.sessionInfoLen)

    # Find the YAML document marker (may have leading null/padding bytes)
    yaml_start = raw_session.find(b"---")
    if yaml_start >= 0:
        session_yaml_raw = raw_session[yaml_start:].rstrip(b"\x00").decode("ascii", errors="replace")
    else:
        session_yaml_raw = raw_session.rstrip(b"\x00").decode("ascii", errors="replace")

    # Parse YAML (use safe_load; iRacing YAML can be quirky)
    try:
        session_yaml = yaml.safe_load(session_yaml_raw)
    except yaml.YAMLError:
        session_yaml = None

    return {
        "header": header,
        "disk_header": disk_header,
        "var_headers": var_headers,
        "var_lookup": var_lookup,
        "session_yaml": session_yaml,
        "session_yaml_raw": session_yaml_raw,
        "path": path,
        "_file": f,
    }


def _read_var_value(data: bytes, vh: cs.Container) -> Any:
    """Read a variable's value(s) from a record buffer slice."""
    vtype = vh.type
    count = vh.count
    offset = vh.offset
    fmt_char = VAR_TYPE_STRUCT_FMT[vtype]
    elem_size = VAR_TYPE_SIZES[vtype]

    if vtype == 0:
        # char array -> return as string
        raw = data[offset : offset + count * elem_size]
        return raw.rstrip(b"\x00").decode("ascii", errors="replace")

    if count == 1:
        return struct.unpack_from(f"<{fmt_char}", data, offset)[0]

    # Array of values
    values = []
    for i in range(count):
        val = struct.unpack_from(f"<{fmt_char}", data, offset + i * elem_size)[0]
        values.append(val)
    return values


def read_record(parsed: dict[str, Any], index: int) -> dict[str, Any]:
    """Read all variables from a single record by index."""
    header = parsed["header"]
    disk_header = parsed["disk_header"]
    f = parsed["_file"]

    if index < 0 or index >= disk_header.sessionRecordCount:
        raise IndexError(
            f"Record index {index} out of range [0, {disk_header.sessionRecordCount})"
        )

    buf_offset = header.varBuf[0].bufOffset
    buf_len = header.bufLen
    f.seek(buf_offset + index * buf_len)
    record_data = f.read(buf_len)

    result = {}
    for vh in parsed["var_headers"]:
        result[vh.name] = _read_var_value(record_data, vh)
    return result


def read_channel(parsed: dict[str, Any], var_name: str) -> list[Any]:
    """Read all values for a named variable across all records."""
    header = parsed["header"]
    disk_header = parsed["disk_header"]
    f = parsed["_file"]

    vh = parsed["var_lookup"].get(var_name)
    if vh is None:
        raise KeyError(f"Variable {var_name!r} not found. Available: {list(parsed['var_lookup'].keys())[:20]}...")

    buf_offset = header.varBuf[0].bufOffset
    buf_len = header.bufLen
    num_records = disk_header.sessionRecordCount
    vtype = vh.type
    var_offset = vh.offset
    count = vh.count
    fmt_char = VAR_TYPE_STRUCT_FMT[vtype]
    elem_size = VAR_TYPE_SIZES[vtype]

    # For efficiency, read all data at once
    total_bytes = num_records * buf_len
    f.seek(buf_offset)
    all_data = f.read(total_bytes)

    values = []
    for i in range(num_records):
        rec_start = i * buf_len + var_offset
        if vtype == 0:
            raw = all_data[rec_start : rec_start + count * elem_size]
            values.append(raw.rstrip(b"\x00").decode("ascii", errors="replace"))
        elif count == 1:
            val = struct.unpack_from(f"<{fmt_char}", all_data, rec_start)[0]
            values.append(val)
        else:
            arr = []
            for j in range(count):
                val = struct.unpack_from(f"<{fmt_char}", all_data, rec_start + j * elem_size)[0]
                arr.append(val)
            values.append(arr)

    return values


def close(parsed: dict[str, Any]) -> None:
    """Close the underlying file handle."""
    f = parsed.get("_file")
    if f and not f.closed:
        f.close()


# ---------------------------------------------------------------------------
# __main__ — diagnostics and reference-value dump
# ---------------------------------------------------------------------------


def main() -> None:
    if len(sys.argv) > 1:
        ibt_path = Path(sys.argv[1])
    else:
        ibt_path = Path(__file__).resolve().parent.parent / "tests" / "test_data" / "test.ibt"

    if not ibt_path.exists():
        print(f"ERROR: File not found: {ibt_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Parsing: {ibt_path}")
    print(f"File size: {ibt_path.stat().st_size:,} bytes")
    print()

    parsed = parse_ibt(ibt_path)
    try:
        hdr = parsed["header"]
        dhdr = parsed["disk_header"]

        # --- Header info ---
        print("=== irsdk_header ===")
        print(f"  ver             = {hdr.ver}")
        print(f"  status          = {hdr.status}")
        print(f"  tickRate        = {hdr.tickRate}")
        print(f"  sessionInfoUpdate = {hdr.sessionInfoUpdate}")
        print(f"  sessionInfoOffset = {hdr.sessionInfoOffset}")
        print(f"  sessionInfoLen  = {hdr.sessionInfoLen}")
        print(f"  numVars         = {hdr.numVars}")
        print(f"  varHeaderOffset = {hdr.varHeaderOffset}")
        print(f"  numBuf          = {hdr.numBuf}")
        print(f"  bufLen          = {hdr.bufLen}")
        for i, vb in enumerate(hdr.varBuf):
            print(f"  varBuf[{i}]: tickCount={vb.tickCount}, bufOffset={vb.bufOffset}")
        print()

        print("=== Disk Sub-Header ===")
        print(f"  sessionStartDate  = {dhdr.sessionStartDate}")
        print(f"  startTime         = {dhdr.startTime}")
        print(f"  endTime           = {dhdr.endTime}")
        print(f"  lapCount          = {dhdr.lapCount}")
        print(f"  sessionRecordCount = {dhdr.sessionRecordCount}")
        print()

        # --- First 10 variable names ---
        print("=== First 10 Variables ===")
        for i, vh in enumerate(parsed["var_headers"][:10]):
            type_name = VAR_TYPE_NAMES.get(vh.type, f"unknown({vh.type})")
            print(
                f"  [{i:3d}] {vh.name:<30s} type={type_name:<10s} "
                f"offset={vh.offset:<6d} count={vh.count}  unit={vh.unit!r}"
            )
        print()

        # --- First 3 records' SessionTime ---
        print("=== First 3 SessionTime values ===")
        for i in range(3):
            rec = read_record(parsed, i)
            print(f"  Record {i}: SessionTime = {rec['SessionTime']}")
        print()

        # --- Session YAML first 20 lines ---
        print("=== Session YAML (first 20 lines) ===")
        yaml_lines = parsed["session_yaml_raw"].split("\n")
        for i, line in enumerate(yaml_lines[:20]):
            print(f"  {i:3d}: {line}")
        print(f"  ... ({len(yaml_lines)} total lines)")
        print()

        # --- Lap values for first 10 records ---
        print("=== Lap values (first 10 records) ===")
        laps = read_channel(parsed, "Lap")
        for i in range(min(10, len(laps))):
            print(f"  Record {i}: Lap = {laps[i]}")
        print()

        # --- Reference value dump as JSON ---
        print("=== Reference values (JSON) ===")
        channels = ["SessionTime", "Speed", "Lap", "RPM", "Gear"]
        sample_indices = [0, 100, 1000, 10000, 50000]

        reference = {}
        for idx in sample_indices:
            if idx >= dhdr.sessionRecordCount:
                print(f"  Skipping record {idx} (only {dhdr.sessionRecordCount} records)")
                continue
            rec = read_record(parsed, idx)
            reference[idx] = {}
            for ch in channels:
                val = rec.get(ch)
                # Convert to JSON-safe types
                if isinstance(val, float):
                    reference[idx][ch] = val
                elif isinstance(val, bool):
                    reference[idx][ch] = val
                elif isinstance(val, int):
                    reference[idx][ch] = val
                else:
                    reference[idx][ch] = str(val)

        print(json.dumps(reference, indent=2))

    finally:
        close(parsed)


if __name__ == "__main__":
    main()
