"""Tests for the libibt.gps utilities."""

import numpy as np
import pytest

from libibt import ibt
from libibt import gps

TEST_FILE = "tests/test_data/test.ibt"


# ── Coordinate conversions ────────────────────────────────────────────


def test_lla_ecef_roundtrip():
    lat, lon, alt = -38.267, 145.238, 12.5  # near Phillip Island
    x, y, z = gps.lla2ecef(lat, lon, alt)
    result = gps.ecef2lla(x, y, z)
    assert result.lat == pytest.approx(lat, abs=1e-9)
    assert result.long == pytest.approx(lon, abs=1e-9)
    assert result.alt == pytest.approx(alt, abs=1e-6)


def test_lla_ecef_roundtrip_arrays():
    lat = np.array([-38.0, 35.0, 50.0])
    lon = np.array([145.0, -120.0, 0.5])
    alt = np.array([0.0, 100.0, 800.0])
    x, y, z = gps.lla2ecef(lat, lon, alt)
    result = gps.ecef2lla(x, y, z)
    np.testing.assert_allclose(result.lat, lat, atol=1e-9)
    np.testing.assert_allclose(result.long, lon, atol=1e-9)
    np.testing.assert_allclose(result.alt, alt, atol=1e-6)


def test_lla2ecef_equator_prime_meridian():
    x, y, z = gps.lla2ecef(0.0, 0.0, 0.0)
    assert x == pytest.approx(6378137.0)  # WGS84 semi-major axis
    assert y == pytest.approx(0.0, abs=1e-9)
    assert z == pytest.approx(0.0, abs=1e-9)


def test_web_mercator_roundtrip():
    lat, lon = -38.267, 145.238
    x, y = gps.llz2web(lat, lon)
    lat2, lon2 = gps.web2ll(x, y)
    assert lat2 == pytest.approx(lat, abs=1e-9)
    assert lon2 == pytest.approx(lon, abs=1e-9)


def test_llz2web_zoom_scales_tiles():
    lat, lon = 10.0, 20.0
    x0, y0 = gps.llz2web(lat, lon, zoom=0)
    x3, y3 = gps.llz2web(lat, lon, zoom=3)
    assert x3 == pytest.approx(x0 * 8)
    assert y3 == pytest.approx(y0 * 8)


def test_ecef_velocity_to_enu():
    # At lat=0, lon=0: ECEF X points up, Y points east, Z points north
    v_east, v_north = gps.ecef_velocity_to_enu(1.0, 2.0, 3.0, 0.0, 0.0)
    assert v_east == pytest.approx(2.0)
    assert v_north == pytest.approx(3.0)


# ── Crossing detection on synthetic data ──────────────────────────────


def test_find_crossing_idx_straight_line():
    # Path going straight north through the marker at index 5
    lats = np.linspace(-0.001, 0.001, 11)
    lons = np.zeros(11)
    XYZ = np.stack(gps.lla2ecef(lats, lons, 0.0), axis=1)
    result = gps.find_crossing_idx(XYZ, (0.0, 0.0))
    idx, dist = result[0], result[1]
    assert idx == pytest.approx(5.0, abs=0.01)
    assert dist < 1.0  # meters


# ── Lap detection on real telemetry ───────────────────────────────────


@pytest.fixture(scope="module")
def log():
    return ibt(TEST_FILE)


def test_find_laps_matches_lap_table(log):
    lat = log.channels["Lat"].column("Lat").to_numpy()
    lon = log.channels["Lon"].column("Lon").to_numpy()
    timecodes = log.channels["Lat"].column("timecodes").to_numpy()

    laps = log.laps.to_pylist()
    # A lap's start is an S/F crossing only if a previous lap in the same
    # session ended there (the first lap starts wherever recording began)
    full_laps = [
        lap
        for i, lap in enumerate(laps)
        if lap["lap_type"] == "full" and i > 0 and laps[i - 1]["session"] == lap["session"]
    ]
    assert full_laps, "test file should contain full laps"

    # Use the car's position at the start of a mid-session full lap as the
    # start/finish marker
    mid_lap = full_laps[len(full_laps) // 2]
    marker_idx = int(np.searchsorted(timecodes, mid_lap["start_time"]))
    marker = (float(lat[marker_idx]), float(lon[marker_idx]))

    XYZ = np.stack(gps.lla2ecef(lat, lon, 0.0), axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        crossings = gps.find_laps(XYZ, timecodes, marker)

    assert crossings, "should detect at least one start/finish crossing"

    # Every full lap's start (an S/F crossing by definition) should have a
    # detected GPS crossing nearby
    for lap in full_laps:
        nearest = min(abs(c - lap["start_time"]) for c in crossings)
        assert nearest < 500, (
            f"No GPS crossing within 500ms of lap {lap['num']} start "
            f"({lap['start_time']}ms); nearest was {nearest}ms away"
        )
