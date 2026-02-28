"""Validate libibt Rust output against the reference Python parser."""

from __future__ import annotations

import sys
from pathlib import Path
from math import isclose

import pytest
import yaml

# Make reference/ importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "reference"))
import ibt_parser  # noqa: E402

from libibt import ibt  # noqa: E402

TEST_IBT = Path(__file__).resolve().parent / "test_data" / "test.ibt"

# Channels to spot-check and which record indices to sample
CHANNELS = ["SessionTime", "Speed", "Lap", "RPM", "Gear"]
SAMPLE_INDICES = [0, 100, 1000, 10000, 50000]


@pytest.fixture(scope="module")
def ref_parsed():
    """Parse the test IBT file with the reference parser."""
    parsed = ibt_parser.parse_ibt(TEST_IBT)
    yield parsed
    ibt_parser.close(parsed)


@pytest.fixture(scope="module")
def rust_logfile():
    """Parse the test IBT file with the Rust-based libibt."""
    return ibt(str(TEST_IBT))


# ── Header values ─────────────────────────────────────────────────────


class TestHeaderValues:
    def test_num_vars(self, ref_parsed, rust_logfile):
        expected = ref_parsed["header"].numVars
        # Rust skips array variables (count > 1) in channels dict,
        # but metadata should still report total numVars via tick_rate presence
        # We check that the Rust metadata exposes the correct header values
        # numVars is not directly in metadata, but we verify the channel count
        # matches the number of scalar (count==1) variables
        scalar_count = sum(1 for vh in ref_parsed["var_headers"] if vh.count == 1)
        assert len(rust_logfile.channels) == scalar_count, (
            f"Channel count mismatch: Rust has {len(rust_logfile.channels)}, "
            f"expected {scalar_count} scalar vars (of {expected} total)"
        )

    def test_tick_rate(self, ref_parsed, rust_logfile):
        expected = ref_parsed["header"].tickRate
        actual = rust_logfile.metadata["tick_rate"]
        assert actual == expected

    def test_record_count(self, ref_parsed, rust_logfile):
        expected = ref_parsed["disk_header"].sessionRecordCount
        actual = rust_logfile.metadata["record_count"]
        assert actual == expected

    def test_lap_count(self, ref_parsed, rust_logfile):
        expected = ref_parsed["disk_header"].lapCount
        actual = rust_logfile.metadata["lap_count"]
        assert actual == expected

    def test_session_start_date(self, ref_parsed, rust_logfile):
        expected = ref_parsed["disk_header"].sessionStartDate
        actual = rust_logfile.metadata["session_start_date"]
        assert actual == expected

    def test_start_time(self, ref_parsed, rust_logfile):
        expected = ref_parsed["disk_header"].startTime
        actual = rust_logfile.metadata["start_time"]
        assert isclose(actual, expected, rel_tol=1e-12)

    def test_end_time(self, ref_parsed, rust_logfile):
        expected = ref_parsed["disk_header"].endTime
        actual = rust_logfile.metadata["end_time"]
        assert isclose(actual, expected, rel_tol=1e-12)


# ── Channel data values ───────────────────────────────────────────────


def _ref_value_at(ref_parsed, channel: str, index: int):
    """Read a single value from the reference parser at a given record index."""
    record = ibt_parser.read_record(ref_parsed, index)
    return record[channel]


def _rust_value_at(rust_logfile, channel: str, index: int):
    """Read a single value from the Rust LogFile at a given record index."""
    table = rust_logfile.channels[channel]
    col = table.column(channel)
    return col[index].as_py()


class TestChannelValues:
    @pytest.mark.parametrize("channel", CHANNELS)
    @pytest.mark.parametrize("index", SAMPLE_INDICES)
    def test_channel_value(self, ref_parsed, rust_logfile, channel, index):
        expected = _ref_value_at(ref_parsed, channel, index)
        actual = _rust_value_at(rust_logfile, channel, index)

        if isinstance(expected, float):
            assert isclose(actual, expected, rel_tol=1e-6, abs_tol=1e-9), (
                f"{channel}[{index}]: Rust={actual}, ref={expected}"
            )
        else:
            assert actual == expected, f"{channel}[{index}]: Rust={actual}, ref={expected}"


# ── Timecodes ─────────────────────────────────────────────────────────


class TestTimecodes:
    @pytest.mark.parametrize("index", SAMPLE_INDICES)
    def test_timecode_matches_session_time(self, ref_parsed, rust_logfile, index):
        """Timecodes should be SessionTime * 1000, rounded to int64 ms."""
        ref_session_time = _ref_value_at(ref_parsed, "SessionTime", index)
        expected_ms = round(ref_session_time * 1000)

        table = rust_logfile.channels["SessionTime"]
        actual_ms = table.column("timecodes")[index].as_py()
        assert actual_ms == expected_ms, (
            f"Timecode[{index}]: Rust={actual_ms}, expected={expected_ms}"
        )


# ── Channel count ─────────────────────────────────────────────────────


class TestChannelPresence:
    def test_all_scalar_channels_present(self, ref_parsed, rust_logfile):
        """Every scalar (count==1) variable from the reference should be a channel."""
        expected_names = {vh.name for vh in ref_parsed["var_headers"] if vh.count == 1}
        actual_names = set(rust_logfile.channels.keys())
        assert actual_names == expected_names

    def test_key_channels_exist(self, rust_logfile):
        for ch in CHANNELS:
            assert ch in rust_logfile.channels, f"Missing channel: {ch}"


# ── Metadata fields ───────────────────────────────────────────────────


class TestMetadata:
    def test_session_info_yaml_parseable(self, rust_logfile):
        raw = rust_logfile.metadata["session_info_yaml"]
        parsed = yaml.safe_load(raw)
        assert parsed is not None
        assert "WeekendInfo" in parsed

    def test_track_name(self, ref_parsed, rust_logfile):
        ref_yaml = ref_parsed["session_yaml"]
        expected = ref_yaml["WeekendInfo"]["TrackName"]
        actual = rust_logfile.metadata["track_name"]
        assert actual == expected

    def test_track_display_name(self, ref_parsed, rust_logfile):
        ref_yaml = ref_parsed["session_yaml"]
        expected = ref_yaml["WeekendInfo"]["TrackDisplayName"]
        actual = rust_logfile.metadata["track_display_name"]
        assert actual == expected

    def test_track_city(self, ref_parsed, rust_logfile):
        ref_yaml = ref_parsed["session_yaml"]
        expected = ref_yaml["WeekendInfo"]["TrackCity"]
        actual = rust_logfile.metadata["track_city"]
        assert actual == expected

    def test_track_country(self, ref_parsed, rust_logfile):
        ref_yaml = ref_parsed["session_yaml"]
        expected = ref_yaml["WeekendInfo"]["TrackCountry"]
        actual = rust_logfile.metadata["track_country"]
        assert actual == expected

    def test_session_yaml_matches_reference(self, ref_parsed, rust_logfile):
        """The raw YAML string from Rust should parse to the same structure."""
        ref_yaml = ref_parsed["session_yaml"]
        rust_yaml = yaml.safe_load(rust_logfile.metadata["session_info_yaml"])
        # Compare top-level keys
        assert set(rust_yaml.keys()) == set(ref_yaml.keys())
        # Spot-check a deeply nested value
        assert (
            rust_yaml["WeekendInfo"]["TrackName"] == ref_yaml["WeekendInfo"]["TrackName"]
        )


# ── Record count consistency ─────────────────────────────────────────


class TestRecordCountConsistency:
    def test_channel_length_matches_record_count(self, ref_parsed, rust_logfile):
        """Every channel table should have exactly sessionRecordCount rows."""
        expected = ref_parsed["disk_header"].sessionRecordCount
        for name, table in rust_logfile.channels.items():
            assert len(table) == expected, (
                f"Channel '{name}' has {len(table)} rows, expected {expected}"
            )
