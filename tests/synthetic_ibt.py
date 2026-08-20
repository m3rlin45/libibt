"""Builder for minimal synthetic IBT files.

The real test file contains no entity-indexed array variables (CarIdx*),
so tests exercise that path with a hand-crafted file. Layout follows the
irsdk disk format: 112-byte header, 32-byte disk sub-header, variable
headers (144 bytes each), session info YAML, then fixed-size records.
"""

import struct

HEADER_SIZE = 112
SUB_HEADER_SIZE = 32
VAR_HEADER_SIZE = 144

# irsdk variable type ids
VT_CHAR = 0
VT_BOOL = 1
VT_INT = 2
VT_BITFIELD = 3
VT_FLOAT = 4
VT_DOUBLE = 5

_TYPE_SIZE = {VT_CHAR: 1, VT_BOOL: 1, VT_INT: 4, VT_BITFIELD: 4, VT_FLOAT: 4, VT_DOUBLE: 8}
_TYPE_FMT = {
    VT_CHAR: "B",
    VT_BOOL: "B",
    VT_INT: "i",
    VT_BITFIELD: "I",
    VT_FLOAT: "f",
    VT_DOUBLE: "d",
}


def build_ibt(
    variables, records, tick_rate=60, session_yaml="---\nWeekendInfo:\n TrackName: synthetic\n"
):
    """Build IBT file bytes.

    Args:
        variables: list of (name, var_type, count, unit, desc). Offsets are
            assigned sequentially. Must include a scalar VT_DOUBLE 'SessionTime'.
        records: list of records; each record is a list with one entry per
            variable — a scalar for count == 1, else a list of count values.
        tick_rate: sample rate written to the header.
        session_yaml: session info YAML text.

    Returns:
        bytes of a parseable IBT file.
    """
    # Assign record-buffer offsets
    offsets = []
    pos = 0
    for _name, var_type, count, _unit, _desc in variables:
        offsets.append(pos)
        pos += _TYPE_SIZE[var_type] * count
    buf_len = pos

    var_header_offset = HEADER_SIZE + SUB_HEADER_SIZE
    session_info_offset = var_header_offset + VAR_HEADER_SIZE * len(variables)
    yaml_bytes = session_yaml.encode()
    session_info_len = len(yaml_bytes)
    buf_offset = session_info_offset + session_info_len

    header = struct.pack(
        "<12i",
        2,  # ver
        0,  # status
        tick_rate,
        0,  # session_info_update
        session_info_len,  # irsdk stores len at offset 16...
        session_info_offset,  # ...and offset at offset 20
        len(variables),  # num_vars
        var_header_offset,
        1,  # num_buf
        buf_len,
        0,
        0,  # pad
    )
    # varBuf[0..3]: (tick_count, buf_offset, pad, pad)
    header += struct.pack("<4i", len(records), buf_offset, 0, 0)
    header += struct.pack("<12i", *([0] * 12))
    assert len(header) == HEADER_SIZE

    session_times = [rec[_index_of(variables, "SessionTime")] for rec in records]
    sub_header = struct.pack(
        "<q2d2i",
        0,  # session_start_date
        float(session_times[0]) if records else 0.0,
        float(session_times[-1]) if records else 0.0,
        0,  # lap_count
        len(records),
    )
    assert len(sub_header) == SUB_HEADER_SIZE

    var_headers = b""
    for (name, var_type, count, unit, desc), offset in zip(variables, offsets):
        vh = struct.pack("<4i", var_type, offset, count, 0)
        vh += name.encode().ljust(32, b"\0")
        vh += desc.encode().ljust(64, b"\0")
        vh += unit.encode().ljust(32, b"\0")
        assert len(vh) == VAR_HEADER_SIZE
        var_headers += vh

    data = b""
    for rec in records:
        rec_bytes = b""
        for (name, var_type, count, _unit, _desc), value in zip(variables, rec):
            values = value if count > 1 else [value]
            assert len(values) == count, f"{name}: expected {count} values"
            rec_bytes += struct.pack(f"<{count}{_TYPE_FMT[var_type]}", *values)
        assert len(rec_bytes) == buf_len
        data += rec_bytes

    return header + sub_header + var_headers + yaml_bytes + data


def _index_of(variables, name):
    for i, var in enumerate(variables):
        if var[0] == name:
            return i
    raise ValueError(f"Variable not found: {name}")
