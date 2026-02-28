"""Cross-validate libibt output against pyirsdk (third-party IBT parser).

pyirsdk is the community-standard Python iRacing SDK maintained at
https://github.com/kutu/pyirsdk. Comparing our Rust parser against it
provides independent validation that we read the binary format correctly.
"""

from __future__ import annotations

from math import isclose
from pathlib import Path

import pytest
import yaml
from irsdk import IBT, IRSDK

from libibt import ibt

TEST_IBT = Path(__file__).resolve().parent / "test_data" / "test.ibt"

# Channels to spot-check and record indices to sample
CHANNELS = ["SessionTime", "Speed", "Lap", "RPM", "Gear"]
SAMPLE_INDICES = [0, 100, 1000, 10000, 50000]


@pytest.fixture(scope="module")
def pyirsdk_ibt():
    """Parse the test IBT file with pyirsdk's IBT reader."""
    reader = IBT()
    reader.open(str(TEST_IBT))
    yield reader
    reader.close()


@pytest.fixture(scope="module")
def pyirsdk_session_yaml():
    """Extract session info YAML from the IBT file using pyirsdk's IRSDK."""
    ir = IRSDK()
    ir.startup(test_file=str(TEST_IBT))
    start = ir._header.session_info_offset
    length = ir._header.session_info_len
    raw = ir._shared_mem[start : start + length].rstrip(b"\x00").decode("cp1252", errors="replace")
    parsed = yaml.safe_load(raw)
    yield parsed
    ir.shutdown()


@pytest.fixture(scope="module")
def rust_logfile():
    """Parse the test IBT file with libibt."""
    return ibt(str(TEST_IBT))


# ── Header values ─────────────────────────────────────────────────────


class TestHeaderValues:
    def test_tick_rate(self, pyirsdk_ibt, rust_logfile):
        expected = pyirsdk_ibt._header.tick_rate
        actual = rust_logfile.metadata["tick_rate"]
        assert actual == expected

    def test_record_count(self, pyirsdk_ibt, rust_logfile):
        expected = pyirsdk_ibt._disk_header.session_record_count
        actual = rust_logfile.metadata["record_count"]
        assert actual == expected

    def test_lap_count(self, pyirsdk_ibt, rust_logfile):
        expected = pyirsdk_ibt._disk_header.session_lap_count
        actual = rust_logfile.metadata["lap_count"]
        assert actual == expected

    def test_session_start_date(self, pyirsdk_ibt, rust_logfile):
        expected = pyirsdk_ibt._disk_header.session_start_date
        actual = rust_logfile.metadata["session_start_date"]
        assert actual == expected

    def test_start_time(self, pyirsdk_ibt, rust_logfile):
        expected = pyirsdk_ibt._disk_header.session_start_time
        actual = rust_logfile.metadata["start_time"]
        assert isclose(actual, expected, rel_tol=1e-12)

    def test_end_time(self, pyirsdk_ibt, rust_logfile):
        expected = pyirsdk_ibt._disk_header.session_end_time
        actual = rust_logfile.metadata["end_time"]
        assert isclose(actual, expected, rel_tol=1e-12)

    def test_num_vars(self, pyirsdk_ibt, rust_logfile):
        """Rust skips array variables, so channel count should match scalar var count."""
        total_vars = pyirsdk_ibt._header.num_vars
        scalar_count = sum(1 for vh in pyirsdk_ibt._var_headers if vh.count == 1)
        assert len(rust_logfile.channels) == scalar_count, (
            f"Channel count mismatch: Rust has {len(rust_logfile.channels)}, "
            f"expected {scalar_count} scalar vars (of {total_vars} total)"
        )


# ── Channel names ─────────────────────────────────────────────────────


class TestChannelNames:
    def test_all_scalar_channels_present(self, pyirsdk_ibt, rust_logfile):
        """Every scalar (count==1) variable from pyirsdk should be in libibt."""
        expected_names = {vh.name for vh in pyirsdk_ibt._var_headers if vh.count == 1}
        actual_names = set(rust_logfile.channels.keys())
        assert actual_names == expected_names

    def test_key_channels_exist(self, rust_logfile):
        for ch in CHANNELS:
            assert ch in rust_logfile.channels, f"Missing channel: {ch}"


# ── Channel data values ──────────────────────────────────────────────


class TestChannelValues:
    @pytest.mark.parametrize("channel", CHANNELS)
    @pytest.mark.parametrize("index", SAMPLE_INDICES)
    def test_channel_value(self, pyirsdk_ibt, rust_logfile, channel, index):
        expected = pyirsdk_ibt.get(index, channel)
        table = rust_logfile.channels[channel]
        actual = table.column(channel)[index].as_py()

        if isinstance(expected, float):
            assert isclose(
                actual, expected, rel_tol=1e-6, abs_tol=1e-9
            ), f"{channel}[{index}]: libibt={actual}, pyirsdk={expected}"
        else:
            assert actual == expected, f"{channel}[{index}]: libibt={actual}, pyirsdk={expected}"


# ── Record count consistency ─────────────────────────────────────────


class TestRecordCount:
    def test_channel_length_matches_record_count(self, pyirsdk_ibt, rust_logfile):
        """Every channel table should have exactly session_record_count rows."""
        expected = pyirsdk_ibt._disk_header.session_record_count
        for name, table in rust_logfile.channels.items():
            assert (
                len(table) == expected
            ), f"Channel '{name}' has {len(table)} rows, expected {expected}"


# ── Session info YAML ────────────────────────────────────────────────


class TestSessionInfo:
    def test_yaml_parseable(self, rust_logfile):
        raw = rust_logfile.metadata["session_info_yaml"]
        parsed = yaml.safe_load(raw)
        assert parsed is not None
        assert "WeekendInfo" in parsed

    def test_top_level_keys_match(self, pyirsdk_session_yaml, rust_logfile):
        rust_yaml = yaml.safe_load(rust_logfile.metadata["session_info_yaml"])
        assert set(rust_yaml.keys()) == set(pyirsdk_session_yaml.keys())

    def test_track_name(self, pyirsdk_session_yaml, rust_logfile):
        expected = pyirsdk_session_yaml["WeekendInfo"]["TrackName"]
        actual = rust_logfile.metadata["track_name"]
        assert actual == expected

    def test_track_display_name(self, pyirsdk_session_yaml, rust_logfile):
        expected = pyirsdk_session_yaml["WeekendInfo"]["TrackDisplayName"]
        actual = rust_logfile.metadata["track_display_name"]
        assert actual == expected

    def test_track_city(self, pyirsdk_session_yaml, rust_logfile):
        expected = pyirsdk_session_yaml["WeekendInfo"]["TrackCity"]
        actual = rust_logfile.metadata["track_city"]
        assert actual == expected

    def test_track_country(self, pyirsdk_session_yaml, rust_logfile):
        expected = pyirsdk_session_yaml["WeekendInfo"]["TrackCountry"]
        actual = rust_logfile.metadata["track_country"]
        assert actual == expected

    def test_weekend_info_matches(self, pyirsdk_session_yaml, rust_logfile):
        """Deeply nested WeekendInfo values should match between parsers."""
        rust_yaml = yaml.safe_load(rust_logfile.metadata["session_info_yaml"])
        rust_wi = rust_yaml["WeekendInfo"]
        pyirsdk_wi = pyirsdk_session_yaml["WeekendInfo"]
        for key in pyirsdk_wi:
            assert (
                rust_wi[key] == pyirsdk_wi[key]
            ), f"WeekendInfo.{key}: libibt={rust_wi[key]!r}, pyirsdk={pyirsdk_wi[key]!r}"
