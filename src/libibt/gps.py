# Copyright 2024, Scott Smith.  MIT License (see LICENSE).
# Adapted for libibt from libxrk / TrackDataAnalysis.

"""GPS utilities for working with iRacing telemetry.

iRacing IBT files carry GPS position as the ``Lat`` (deg), ``Lon`` (deg),
and ``Alt`` (m) channels. These helpers convert between coordinate systems
and detect lap crossings from a start/finish marker:

    >>> import numpy as np
    >>> from libibt import ibt
    >>> from libibt import gps
    >>> log = ibt('session.ibt')
    >>> lat = log.channels['Lat'].column('Lat').to_numpy()
    >>> lon = log.channels['Lon'].column('Lon').to_numpy()
    >>> tc = log.channels['Lat'].column('timecodes').to_numpy()
    >>> XYZ = np.stack(gps.lla2ecef(lat, lon, 0.0), axis=1)
    >>> crossings = gps.find_laps(XYZ, tc, marker=(lat[0], lon[0]))

Note: ``find_laps`` and ``find_crossing_idx`` treat the track as being at
altitude 0, so build ECEF coordinates with ``alt=0`` rather than the real
altitude channel.
"""

from collections import namedtuple

import numpy as np

GPS = namedtuple("GPS", ["lat", "long", "alt"])


# convert lat/long/zoom to web mercator. lat/long are degrees
# returns x,y as floats - integer component is which tile to download
def llz2web(lat, long, zoom=0):
    # wikipedia web mercator projection
    mult = 0.25 * (2 << zoom)
    return (
        mult * (1 + long / 180),
        mult * (1 - np.log(np.tan(np.pi / 4 + np.pi / 360 * lat)) / np.pi),
    )


# returns lat/long as floats in degrees
def web2ll(x, y, zoom=0):
    mult = 1 / (0.25 * (2 << zoom))
    return (
        np.arctan(np.exp(np.pi - np.multiply(np.pi * mult, y))) * 360 / np.pi - 90,
        np.multiply(180 * mult, x) - 180,
    )


# lat, long = degrees
# x, y, z, alt = meters
def lla2ecef(lat, lon, alt):
    a = 6378137
    e = 8.181919084261345e-2
    e_sq = e * e

    lat = lat * (np.pi / 180)
    lon = lon * (np.pi / 180)

    clat = np.cos(lat)
    slat = np.sin(lat)

    N = a / np.sqrt(1 - e_sq * slat * slat)

    x = (N + alt) * clat * np.cos(lon)
    y = (N + alt) * clat * np.sin(lon)
    z = ((1 - e_sq) * N + alt) * slat

    return x, y, z


# Computing geodetic coordinates from geocentric coordinates
# H. Vermeille, 2003/2004
# http://users.auth.gr/kvek/78_Vermeille.pdf
def ecef2lla_vermeille2003(x, y, z):
    a = 6378137.0
    e = 8.181919084261345e-2

    p = (x * x + y * y) * (1 / (a * a))
    q = ((1 - e * e) / (a * a)) * z * z
    r = (p + q - e**4) * (1 / 6)
    s = (e**4 / 4) * p * q / (r**3)
    p = None
    t = np.cbrt(1 + s + np.sqrt(s * (2 + s)))
    s = None
    u = r * (1 + t + 1 / t)
    r = None
    t = None
    v = np.sqrt(u * u + e**4 * q)
    u += v  # precalc
    w = (e**2 / 2) * (u - q) / v
    q = None
    k = np.sqrt(u + w * w) - w
    D = k * np.sqrt(x * x + y * y) / (k + e**2)
    rtDDzz = np.sqrt(D * D + z * z)
    return GPS(
        (180 / np.pi) * 2 * np.arctan2(z, D + rtDDzz),
        (180 / np.pi) * np.arctan2(y, x),
        (k + e**2 - 1) / k * rtDDzz,
    )


ecef2lla = ecef2lla_vermeille2003


def find_crossing_idx(
    XYZ: np.ndarray, marker: np.ndarray  # coordinates to look up in (X, Y, Z), meters
):  # (lat, long), degrees

    if isinstance(marker, tuple):
        marker = np.array(marker)
    if len(marker.shape) == 1:
        # force it to be a 2d shape to make the rest of the code simpler
        return find_crossing_idx(XYZ, marker.reshape((1, len(marker))))[0]

    # very similar to gps lap insert, but we can assume XYZ is a
    # reference (as opposed to GPS lap insert where we aren't sure if
    # the trajectory of the GPS is correct or not - think pit stops,
    # going off track, etc).  As a result, only one pass is needed.
    # Also we do not need to filter based on minspeed, so no timecodes
    # are used in this function.

    lat = marker[:, 0].reshape((len(marker), 1))
    lon = marker[:, 1].reshape((len(marker), 1))
    SO = np.stack(lla2ecef(lat, lon, 0), axis=2)
    SD = np.stack(lla2ecef(lat, lon, 1000), axis=2) - SO

    O = XYZ[:, :3]
    D = O[1:] - O[:-1]
    O = O[:-1] - SO

    SN = np.sum(SD * SD, axis=2, keepdims=True) * D - np.sum(SD * D, axis=2, keepdims=True) * SD
    t = np.clip(
        -np.sum(SN * O, axis=2, keepdims=True) / np.sum(SN * D, axis=2, keepdims=True),
        0,
        1,
    )

    # XXX This won't work with rally stages (anything not a circuit)
    distsq = np.sum(np.square(O + t * D), axis=2)
    minidx = np.argmin(distsq, axis=1)
    colrange = np.arange(t.shape[0])
    return np.column_stack([minidx + t[colrange, minidx, 0], np.sqrt(distsq[colrange, minidx])])


def find_crossing_dist(
    XYZD: np.ndarray,  # coordinates to look up in (X, Y, Z, Distance), meters
    marker: tuple[float, float],
):  # (lat, long) tuple, degrees
    idx, _ = find_crossing_idx(XYZD, np.array(marker))
    if idx + 1 >= len(XYZD):
        return XYZD[int(idx), 3]
    scale, idx = np.modf(idx)
    return XYZD[int(idx), 3] + scale * (XYZD[int(idx) + 1, 3] - XYZD[int(idx), 3])


def find_laps(
    XYZ: np.ndarray,  # coordinates to look up in (X, Y, Z), meters
    timecodes: np.ndarray,  # time for above coordinates, ms
    marker: tuple[float, float],
):  # (lat, long) tuple, degrees
    # gps lap insert.  We assume the start finish "line" is a
    # plane containing the vector that goes through the GPS
    # coordinates sf lat/long from altitude 0 to 1000.  The normal
    # of the plane is generally in line with the direction of
    # travel, given the above constraint.

    # O, D = vehicle vector (O=origin, D=direction, [0]=O, [1]=O+D)
    # SO, SD = start finish origin, direction (plane must contain SO and SO+SD poitns)
    # SN = start finish plane normal

    # D = a*SD + SN
    # 0 = SD . SN
    # combine to get: 0 = SD . (D - a*SD)
    #                  a * (SD . SD) = SD . D
    # plug back into first eq:
    # SN = D - (SD . D) / (SD . SD) * SD
    # or to avoid division, and because length doesn't matter:
    # SN = (SD . SD) * D - (SD. D) * SD

    # now determine intersection with plane SO,SN from vector O,O+D:
    # SN . (O + tD - SO) = 0
    # t * (D . SN) + SN . (O - SO) = 0
    # t = -SN.(O-SO) / D.SN

    SO = np.array(lla2ecef(*marker, 0.0)).reshape((1, 3))
    SD = np.array(lla2ecef(*marker, 1000)).reshape((1, 3)) - SO

    O = XYZ - SO

    D = O[1:] - O[:-1]
    O = O[:-1]

    # VBox seems to need 30, maybe my friend is using an old map description
    marker_size = 30  # meters, how far you can be from the marker to count as a lap

    # Precalculate in which time periods we were traveling at least 4 m/s (~10mph)
    minspeed = np.sum(D * D, axis=1) > np.square((timecodes[1:] - timecodes[:-1]) * (4 / 1000))

    SN = (
        np.sum(SD * SD, axis=1).reshape((len(SD), 1)) * D
        - np.sum(SD * D, axis=1).reshape((len(D), 1)) * SD
    )
    t = np.maximum(-np.sum(SN * O, axis=1) / np.sum(SN * D, axis=1), 0)
    # This only works because the track is considered at altitude 0
    dist = np.sum(np.square(O + t.reshape((len(t), 1)) * D), axis=1)
    pick = (t[1:] <= 1) & (t[:-1] > 1) & (dist[1:] < marker_size**2)

    # Now that we have a decent candidate selection of lap
    # crossings, generate a single normal vector for the
    # start/finish line to use for all lap crossings, to make the
    # lap times more accurate/consistent.  Weight the crossings by
    # velocity and add them together.  As it happens, SN is
    # already weighted by velocity...
    SN = np.sum(SN[1:][pick & minspeed[1:]], axis=0).reshape((1, 3))
    # recompute t, dist, pick
    t = np.maximum(-np.sum(SN * O, axis=1) / np.sum(SN * D, axis=1), 0)
    dist = np.sum(np.square(O + t.reshape((len(t), 1)) * D), axis=1)
    pick = (t[1:] <= 1) & (t[:-1] > 1) & (dist[1:] < marker_size**2)

    lap_markers = [0]
    for idx in np.nonzero(pick)[0] + 1:
        if timecodes[idx] <= lap_markers[-1]:
            continue
        if not minspeed[idx]:
            idx = np.argmax(minspeed[idx:]) + idx
        lap_markers.append(timecodes[idx] + t[idx] * (timecodes[idx + 1] - timecodes[idx]))
    return lap_markers[1:]


def ecef_velocity_to_enu(dX, dY, dZ, lat_rad, lon_rad):
    """Convert ECEF velocity to ENU (East-North-Up) frame.

    Args:
        dX, dY, dZ: Velocity components in ECEF frame (e.g., cm/s)
        lat_rad: Latitude in radians
        lon_rad: Longitude in radians

    Returns:
        V_east, V_north: Velocity components in local ENU frame (same units as input)
    """
    sin_lat, cos_lat = np.sin(lat_rad), np.cos(lat_rad)
    sin_lon, cos_lon = np.sin(lon_rad), np.cos(lon_rad)

    V_east = -sin_lon * dX + cos_lon * dY
    V_north = -sin_lat * cos_lon * dX - sin_lat * sin_lon * dY + cos_lat * dZ
    return V_east, V_north
