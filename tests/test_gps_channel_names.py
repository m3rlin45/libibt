"""Tests for the GPS_CHANNEL_NAMES export."""

from libibt import ibt, GPS_CHANNEL_NAMES

TEST_FILE = "tests/test_data/test.ibt"


def test_gps_channel_names_exported():
    assert GPS_CHANNEL_NAMES == ("Lat", "Lon", "Alt")


def test_gps_channels_present_in_file():
    log = ibt(TEST_FILE)
    for name in GPS_CHANNEL_NAMES:
        assert name in log.channels, f"Missing GPS channel: {name}"
    gps_log = log.select_channels(GPS_CHANNEL_NAMES)
    assert set(gps_log.channels.keys()) == set(GPS_CHANNEL_NAMES)
