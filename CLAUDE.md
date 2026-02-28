# libibt — Agent Instructions

## Project overview

Python library for reading iRacing IBT telemetry files. Rust core parses the binary format, PyO3 bridges to Python, and a Python `LogFile` dataclass provides the user-facing API. Data is returned as PyArrow tables.

## Source control

**Use `sl` (Sapling) only. Never use `git`.**

## Architecture

Cargo workspace with two crates: a publishable pure-Rust core and PyO3 bindings.

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

crates/libibt-python/                    # PyO3 bindings (publish = false)
  src/
    lib.rs          — ibt() entry point, open_source(), build_metadata()

src/libibt/
  __init__.py     — Public API: ibt(), LogFile
  base.py         — LogFile dataclass (channels, laps, metadata, filtering, resampling)
  _libibt_rs.pyi  — Type stubs for the Rust extension

tests/
  test_parse.py     — Basic parsing tests
  test_reference.py — Cross-validation against reference parser
  test_channels.py  — Channel data tests
  test_logfile.py   — LogFile method tests
```

## Build and test

```bash
uv sync                  # Install dependencies
just build               # Build Rust extension (release)
just build-debug         # Build Rust extension (debug, faster)
just test                # uv run pytest tests/ -v
just check               # All checks: lint, clippy, typecheck, test-rust, test
just format              # Black + cargo fmt
just typecheck           # mypy src/
just clippy              # Rust lints
```

Always use `uv run` for Python commands, never bare `python` or `pytest`.

## Key details

- **Python >=3.10**, Rust 2021 edition
- **Dependencies**: pyarrow, numpy (version-constrained by Python version)
- **Build system**: maturin (configured in pyproject.toml)
- **Formatter**: Black (line-length 100)
- **Type checker**: mypy (strict optional, check untyped defs)
- **Timecodes**: int64 milliseconds throughout
- **Channels**: each is a 2-column PyArrow table (`timecodes` + value), with field metadata (units, desc, interpolate)
- **Laps**: PyArrow table with columns `num`, `start_time`, `end_time` (all ms)
- **Array variables** (count > 1) are not yet supported — only scalar variables become channels
- **LogFile methods are immutable** — filtering/resampling returns new instances
