# libibt task runner

# Format code (Python + Rust)
format:
    uv run black .
    cargo fmt

# Check formatting (Python + Rust)
lint:
    uv run black --check .
    cargo fmt --check

# Rust clippy lints
clippy:
    cargo clippy --workspace --all-targets -- -W clippy::all

# Type check with mypy
typecheck:
    uv run mypy src/

# Run Rust tests
test-rust:
    cargo test --workspace

# Run Python tests
test:
    uv run pytest tests/ -v

# Run all checks (lint, clippy, typecheck, test-rust, test)
check: lint clippy typecheck test-rust test

# Build Rust extension module (release)
build:
    #!/usr/bin/env bash
    source $HOME/.cargo/env && uv run maturin develop --release

# Build Rust extension module (debug, faster compile)
build-debug:
    #!/usr/bin/env bash
    source $HOME/.cargo/env && uv run maturin develop

# Interactive REPL with a loaded IBT file
repl:
    uv run python -i -c "from libibt import ibt; log = ibt('formulair04_phillipisland 2026-02-27 20-40-38.ibt'); print('IBT file loaded as: log'); print(f'Channels: {len(log.channels)}'); print(f'Laps: {len(log.laps)}'); print(f'Metadata keys: {list(log.metadata.keys())}')"
