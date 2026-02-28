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

# Python 3.13 path for Pyodide builds
_py313 := `uv python find 3.13`

# Set up Emscripten SDK for Pyodide 0.29.x
emsdk-setup:
    #!/usr/bin/env bash
    set -euo pipefail
    PY313="{{ _py313 }}"
    VENV=build/pyodide-venv
    mkdir -p build
    [ -d "$VENV" ] || uv venv --seed --python="$PY313" "$VENV"
    uv pip install --prerelease=allow --python="$VENV/bin/python" "wheel<0.44.0" pyodide-build==0.29.3
    source "$VENV/bin/activate"
    pyodide xbuildenv install 0.29.3
    EMSDK_VERSION=$(pyodide config get emscripten_version)
    echo "Emscripten version: $EMSDK_VERSION"
    [ -d build/emsdk/.git ] || git clone https://github.com/emscripten-core/emsdk.git build/emsdk
    cd build/emsdk && git config core.autocrlf false && git checkout -- .
    ./emsdk install "$EMSDK_VERSION"
    ./emsdk activate "$EMSDK_VERSION"

# Install Pyodide npm package
pyodide-npm-setup:
    npm install pyodide@0.29.3

# Build Pyodide wheel (wasm32-unknown-emscripten)
pyodide-build: emsdk-setup
    #!/usr/bin/env bash
    set -euo pipefail
    _bak=/tmp/_libibt_native_$$ && mkdir -p "$_bak"
    find src/libibt -maxdepth 1 -name '*linux-gnu.so' -exec mv {} "$_bak/" \; 2>/dev/null || true
    EMSDK=$PWD/build/emsdk
    VENV=$PWD/build/pyodide-venv
    export PATH="$VENV/bin:$HOME/.cargo/bin:$EMSDK/upstream/emscripten:$PATH"
    export EMSDK EM_CONFIG=$EMSDK/.emscripten
    export RUSTUP_TOOLCHAIN=nightly RUSTFLAGS="-Zemscripten-wasm-eh"
    export CARGO_BUILD_TARGET=wasm32-unknown-emscripten
    pyodide build --exports whole_archive
    _rc=$?
    mv "$_bak"/*.so src/libibt/ 2>/dev/null || true
    rm -rf "$_bak"
    [ $_rc -eq 0 ] && echo "Wheel built:" && ls dist/*wasm32*.whl
    exit $_rc

# Build and test with Pyodide (wasm32-unknown-emscripten)
pyodide-test: pyodide-build pyodide-npm-setup
    #!/usr/bin/env bash
    set -euo pipefail
    node scripts/run_pyodide_tests.mjs --dist-dir=./dist
    node scripts/run_pyodide_tests_idbfs.mjs --dist-dir=./dist
