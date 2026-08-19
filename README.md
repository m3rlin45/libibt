# libibt

[![PyPI](https://img.shields.io/pypi/v/libibt)](https://pypi.org/project/libibt/)
[![Python](https://img.shields.io/pypi/pyversions/libibt)](https://pypi.org/project/libibt/)

A Python library for reading iRacing IBT telemetry files. Rust core with PyO3 bindings for fast parsing; returns data as PyArrow tables.

## Installation

```bash
pip install libibt
```

or with [uv](https://docs.astral.sh/uv/):

```bash
uv add libibt
```

### From source

```bash
uv sync
just build
```

Requires a Rust toolchain (for maturin) and [uv](https://docs.astral.sh/uv/).

## Usage

```python
from libibt import ibt

# Load from file path, PathLike, bytes-like (bytes, bytearray, memoryview),
# or file-like object
log = ibt('session.ibt')

print(log)
# LogFile(file_name='session.ibt', channels=142, laps=12)

# Access a single channel (PyArrow table with 'timecodes' + value columns)
speed = log.channels['Speed']
print(speed.column_names)  # ['timecodes', 'Speed']

# Merge all channels into one table
table = log.get_channels_as_table()
df = table.to_pandas()

# Access laps (PyArrow table: num, start_time, end_time — all in ms)
for i in range(log.laps.num_rows):
    lap = log.laps.column("num")[i].as_py()
    start = log.laps.column("start_time")[i].as_py()
    end = log.laps.column("end_time")[i].as_py()
    print(f"Lap {lap}: {start} - {end}")

# Session metadata
print(log.metadata['track_name'])
print(log.metadata['session_info_yaml'])  # Full iRacing session YAML
```

### Filtering and resampling

```python
from libibt import ibt

log = ibt('session.ibt')

# Select specific channels
subset = log.select_channels(['Speed', 'Throttle', 'Brake'])

# Filter to a time range (ms, inclusive start, exclusive end)
segment = log.filter_by_time_range(60000, 120000)

# Filter to a specific lap
lap5 = log.filter_by_lap(5)

# Combine filtering and channel selection
lap5_speed = log.filter_by_lap(5, channel_names=['Speed', 'Throttle'])

# Resample all channels to match a reference channel's timebase
aligned = log.resample_to_channel('Speed')

# Resample to a custom timebase
import pyarrow as pa
target = pa.array(range(0, 100000, 100), type=pa.int64())  # 10 Hz
resampled = log.resample_to_timecodes(target)

# Chain operations
df = (log
    .filter_by_lap(5)
    .select_channels(['Speed', 'RPM', 'Gear'])
    .get_channels_as_table()
    .to_pandas())
```

All methods return new `LogFile` instances (immutable pattern).

### Progress callback

```python
def on_progress(current, total):
    print(f"Channel {current}/{total}")

log = ibt('session.ibt', progress=on_progress)
```

### Channel metadata

Each channel carries typed metadata via the `ChannelMetadata` dataclass:

```python
from libibt import ibt, ChannelMetadata

log = ibt('session.ibt')

# Extract from a channel table
meta = ChannelMetadata.from_channel_table(log.channels['Speed'])
print(meta.units)        # 'm/s'
print(meta.desc)         # 'GPS based speed'
print(meta.interpolate)  # True

# Or from a PyArrow field directly
field = log.channels['Speed'].schema.field('Speed')
meta = ChannelMetadata.from_field(field)

# Serialize back to PyArrow field metadata
raw = meta.to_field_metadata()  # dict[bytes, bytes]
```

`ChannelMetadata` is a frozen (immutable) dataclass with fields:

| Field | Type | Description |
|-------|------|-------------|
| `units` | `str` | Unit string (e.g., `"m/s"`, `"revs/min"`) |
| `desc` | `str` | Channel description |
| `interpolate` | `bool` | Whether to use linear interpolation when resampling |

Metadata is preserved through all `LogFile` operations (filtering, resampling, merging).

### Metadata fields

`log.metadata` contains fields from both the binary header and parsed session YAML:

| Key | Description |
|-----|-------------|
| `session_info_yaml` | Full iRacing session info YAML |
| `session_info` | Parsed YAML as a dict |
| `track_name` | Track internal name |
| `track_display_name` | Track display name |
| `track_city` | Track city |
| `track_country` | Track country |
| `track_length` | Track length |
| `track_id` | Track ID |
| `track_type` | Track type |
| `series_id`, `season_id`, `session_id`, `sub_session_id` | iRacing IDs |
| `tick_rate` | Sample rate (typically 60 Hz) |
| `record_count` | Total number of records |
| `lap_count` | Number of laps |
| `session_start_date` | Session start date |
| `start_time`, `end_time` | Session time bounds |
| `event_type` | Event type (e.g., "Race") |
| `session_type`, `session_name` | Current session type and name |
| `driver_name` | Recording driver's username |
| `driver_user_id`, `driver_irating`, `driver_license` | Driver details |
| `car_name`, `car_id` | Car screen name and ID |
| `car_gear_count`, `car_redline_rpm`, `car_shift_rpm`, `car_idle_rpm` | Car specs |
| `num_drivers` | Number of drivers (excluding pace car) |
| `weather_temp`, `weather_surface_temp`, `weather_humidity` | Weather conditions |
| `weather_skies`, `weather_wind_speed`, `weather_wind_dir` | Weather conditions |
| `car_setup` | Full car setup dict |
| `sectors` | Sector split definitions |

### Array variables

Time-subsample arrays (`_ST` suffix, recorded at `tick_rate × count` Hz — e.g. 6 samples per 60 Hz tick = 360 Hz) are merged into a single higher-rate channel under the variable's name:

```python
torque = log.channels['SteeringWheelTorque_ST']  # 360 Hz, 6x the rows
```

Sub-sample timecodes are interleaved between ticks, with the last sub-sample of each tick aligned with the tick's timestamp.

Other array variables (count > 1 where the index is not time, e.g. the per-car `CarIdx` arrays) become vector-valued channels: one channel under the variable's name whose value column is a PyArrow `FixedSizeList`, one vector per tick — the same array-per-sample model pyirsdk and the official SDK use:

```python
import numpy as np

col = log.channels['CarIdxLapDistPct'].column('CarIdxLapDistPct').combine_chunks()
values = col.values.to_numpy().reshape(-1, col.type.list_size)  # (rows, 64)
car12 = values[:, 12]  # one car's trace
```

Vector channels work with all `LogFile` operations, including resampling (interpolation and forward-fill are applied per element). All channels carry the variable's metadata (units, desc, interpolate).

### GPS utilities

IBT files carry GPS position as the `Lat` (deg), `Lon` (deg), and `Alt` (m) channels — available as the `GPS_CHANNEL_NAMES` constant:

```python
from libibt import ibt, GPS_CHANNEL_NAMES

log = ibt('session.ibt')
gps_log = log.select_channels(GPS_CHANNEL_NAMES)
```

The `libibt.gps` module provides coordinate conversions and GPS-based lap detection:

```python
import numpy as np
from libibt import ibt
from libibt import gps

log = ibt('session.ibt')
lat = log.channels['Lat'].column('Lat').to_numpy()
lon = log.channels['Lon'].column('Lon').to_numpy()
timecodes = log.channels['Lat'].column('timecodes').to_numpy()

# Convert to ECEF coordinates (meters). Lap detection treats the track as
# altitude 0, so pass alt=0 rather than the Alt channel.
XYZ = np.stack(gps.lla2ecef(lat, lon, 0.0), axis=1)

# Detect start/finish crossings from a (lat, lon) marker — returns crossing
# times in ms, interpolated between samples
crossings = gps.find_laps(XYZ, timecodes, marker=(lat[0], lon[0]))

# Other helpers
gps.llz2web(lat, lon)            # lat/lon -> web mercator tile coordinates
gps.web2ll(x, y)                 # web mercator -> lat/lon
gps.ecef2lla(x, y, z)            # ECEF -> (lat, long, alt) namedtuple
gps.find_crossing_idx(XYZ, m)    # fractional index where a path crosses a marker
gps.ecef_velocity_to_enu(dx, dy, dz, lat_rad, lon_rad)  # -> (east, north)
```

## Development

```bash
# Build Rust extension (release)
just build

# Build (debug, faster compile)
just build-debug

# Run tests
just test

# Run all checks (format, clippy, typecheck, tests)
just check

# Format code
just format
```

## Credits

The GPS utilities are adapted from [libxrk](https://github.com/m3rlin45/libxrk), which incorporates code from [TrackDataAnalysis](https://github.com/racer-coder/TrackDataAnalysis) by Scott Smith, used under the MIT License.

## License

MIT
