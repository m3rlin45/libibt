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

# Load from file path, bytes, PathLike, or file-like object
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

Each channel table carries metadata on the value field:

```python
field = log.channels['Speed'].schema.field('Speed')
print(field.metadata[b'units'])        # e.g. b'm/s'
print(field.metadata[b'desc'])         # e.g. b'GPS based speed'
print(field.metadata[b'interpolate'])  # b'True' or b'False'
```

### Metadata fields

`log.metadata` contains:

| Key | Description |
|-----|-------------|
| `session_info_yaml` | Full iRacing session info YAML |
| `track_name` | Track internal name |
| `track_display_name` | Track display name |
| `track_city` | Track city |
| `track_country` | Track country |
| `track_length` | Track length |
| `series_id`, `season_id`, `session_id`, `sub_session_id` | iRacing IDs |
| `tick_rate` | Sample rate (typically 60 Hz) |
| `record_count` | Total number of records |
| `lap_count` | Number of laps |
| `session_start_date` | Session start date |
| `start_time`, `end_time` | Session time bounds |

## Limitations

Array variables (count > 1, e.g. tire temperature arrays) are not yet supported. Only scalar variables are returned as channels.

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

## License

MIT
