# libibt — Agent Instructions

## Project overview

Python library for reading iRacing IBT telemetry files. Rust core parses the binary format, PyO3 bridges to Python, and a Python `LogFile` dataclass provides the user-facing API. Data is returned as PyArrow tables.

## Source control

**Use `sl` (Sapling) only. Never use `git`.**

## Architecture

Cargo workspace with three crates: a publishable pure-Rust core, PyO3 bindings, and a C ABI.

```
Cargo.toml                              # Workspace root

crates/libibt/                           # Pure Rust core (publishable to crates.io)
  src/
    lib.rs          — Public API: re-exports, read_ibt(), read_ibt_file()
    error.rs        — Error types (Arrow variant feature-gated)
    header.rs       — Binary header structs
    var_header.rs   — Variable header parsing (name, type, offset)
    reader.rs       — IbtFile: mmap, header parsing, channel extraction
    channel.rs      — Arrow RecordBatch construction (feature-gated behind `arrow`)
    session_info.rs — Session YAML extraction
    session_yaml.rs — Session-info YAML loader with PyYAML's (YAML 1.1) typing
    metadata.rs     — Metadata document: keys, order and values shared by every binding, incl. the parsed YAML and the entries derived from it

crates/libibt-python/                    # PyO3 bindings (publish = false)
  src/
    lib.rs          — ibt() entry point, open_source(), build_metadata()

crates/libibt-ffi/                       # C ABI for non-Python consumers (publish = false)
  src/
    lib.rs          — ibt_* functions: sessions, Arrow C streams, metadata JSON
  tests/
    roundtrip.rs    — Drives the C ABI like a foreign caller

src/libibt/
  __init__.py     — Public API: ibt(), ChannelMetadata, LogFile
  base.py         — ChannelMetadata frozen dataclass, LogFile dataclass
  _libibt_rs.pyi  — Type stubs for the Rust extension

tests/
  test_parse.py             — Basic parsing tests
  test_reference.py         — Cross-validation against reference parser
  test_cross_validation.py  — Cross-validation against pyirsdk
  test_channels.py          — Channel data tests
  test_channel_metadata.py  — ChannelMetadata roundtrip and preservation tests
  test_logfile.py           — LogFile method tests
  test_session_yaml.py      — Rust YAML loader and derived metadata vs PyYAML
  test_ffi.py               — C ABI vs LogFile parity (needs LIBIBT_FFI_LIB)
```

## Build and test

```bash
uv sync                  # Install dependencies
just build               # Build Rust extension (release)
just build-debug         # Build Rust extension (debug, faster)
just test                # uv run pytest tests/ -v -n auto
just check               # All checks: lint, clippy, typecheck, test-rust, test
just format              # Black + cargo fmt
just typecheck           # mypy src/
just clippy              # Rust lints
```

Always use `uv run` for Python commands, never bare `python` or `pytest`.

## C ABI (`crates/libibt-ffi`)

`libibt-ffi` builds the Rust core as a shared library (`ibt_ffi`) for callers
that are not Python; the C# Inferno Analyzer in motorsports_data_notebook is
the consumer. A session is opened from a path or bytes, each channel and the
laps table come out through the Arrow C Data Interface (`ArrowArrayStream`),
and the metadata comes out as JSON. The ten `ibt_*` functions and their
ownership rules are documented in `crates/libibt-ffi/src/lib.rs`.

**The contract is "what the Python `LogFile` holds":** same channels in the
same order, same schemas and field metadata, same laps, and the `metadata`
dict as JSON with the same keys in the same order (dates as ISO 8601 strings).
`tests/test_ffi.py` asserts that on every fixture by loading the library with
ctypes; it only runs when `LIBIBT_FFI_LIB` points at the library.

```bash
just ffi-build       # target/release/libibt_ffi.so (.dylib, or ibt_ffi.dll)
just ffi-test        # build, then tests/test_ffi.py against the library
just test-rust       # cargo tests, including the C ABI crate's round trip
```

Keep the bindings thin. Anything every binding has to agree on lives in the
core crate, not in `libibt-python`, `libibt-ffi` or `base.py`: the whole
metadata document (`IbtFile::metadata_entries`, parsed session info included),
and the channel and laps batches (`all_channels_to_arrow`, `extract_laps`). Changing an `ibt_*`
signature breaks the consumer, which builds this crate from a pinned commit.

## Key details

- **Python >=3.10**, Rust 2021 edition
- **Dependencies**: pyarrow, numpy (version-constrained by Python version). PyYAML is a test dependency only: the session-info YAML is parsed in Rust
- **Build system**: maturin (configured in pyproject.toml)
- **Formatter**: Black (line-length 100)
- **Type checker**: mypy (strict optional, check untyped defs)
- **Timecodes**: int64 milliseconds throughout
- **Channels**: each is a 2-column PyArrow table (`timecodes` + value), with field metadata accessed via `ChannelMetadata` (frozen dataclass with `units: str`, `desc: str`, `interpolate: bool`; constructed via `from_field()`, `from_channel_table()`, serialized via `to_field_metadata()`)
- **Laps**: PyArrow table with columns `num`, `start_time`, `end_time` (all ms)
- **Array variables** (count > 1): `_ST` time-subsample arrays are merged into one higher-rate channel under the variable's name; other arrays (e.g. per-car `CarIdx*`) become vector-valued channels whose value column is a `FixedSizeList` (one vector per tick)
- **LogFile methods are immutable** — filtering/resampling returns new instances
- **Metadata is built in Rust** (`IbtFile::metadata_entries`), including `session_info` (the parsed session-info YAML) and the keys derived from it. `session_yaml.rs` reproduces PyYAML's `safe_load` typing (`yes`/`off` are bools, `1:30` is 90, `2026-02-27` is a date); `tests/test_session_yaml.py` checks it against PyYAML. Change the typing rules only together with that test
