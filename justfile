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

# Python 3.13 path for Pyodide 0.29 builds
_py313 := `uv python find 3.13`

# pyodide-build is a BUILD TOOL; its version is independent of the Pyodide
# RUNTIME version. Pinning it to the runtime version (==0.29.3) broke builds:
# those releases hardcode a cross-build-env metadata URL upstream has removed.
# One current pyodide-build serves every runtime.
_pyodide_build_version := "0.39.0"

# Set up Emscripten SDK for Pyodide 0.29.x
emsdk-setup:
    #!/usr/bin/env bash
    set -euo pipefail
    PY313="{{ _py313 }}"
    VENV=build/pyodide-venv
    mkdir -p build
    [ -d "$VENV" ] || uv venv --seed --python="$PY313" "$VENV"
    uv pip install --prerelease=allow --python="$VENV/bin/python" "wheel<0.44.0" pyodide-build=={{ _pyodide_build_version }}
    source "$VENV/bin/activate"
    pyodide xbuildenv install 0.29.3
    EMSDK_VERSION=$(pyodide config get emscripten_version)
    echo "Emscripten version: $EMSDK_VERSION"
    [ -d build/emsdk/.git ] || git clone https://github.com/emscripten-core/emsdk.git build/emsdk
    cd build/emsdk && git config core.autocrlf false && git checkout -- .
    ./emsdk install "$EMSDK_VERSION"
    ./emsdk activate "$EMSDK_VERSION"
    WASM_OPT=upstream/bin/wasm-opt
    [ -f ${WASM_OPT}.real ] || mv $WASM_OPT ${WASM_OPT}.real
    sed 's/\r$//' ../../scripts/wasm-opt-wrapper.sh > $WASM_OPT && chmod +x $WASM_OPT

# Install Pyodide npm packages
pyodide-npm-setup:
    npm install pyodide-0.29@npm:pyodide@0.29.3

# Build Pyodide 0.29 wheel (legacy pyodide_2025_0 tag -> GitHub Releases)
pyodide-build: emsdk-setup
    #!/usr/bin/env bash
    set -euo pipefail
    _bak=/tmp/_libibt_native_$$ && mkdir -p "$_bak"
    find src/libibt -maxdepth 1 -name '*linux-gnu.so' -exec mv {} "$_bak/" \; 2>/dev/null || true
    rm -f dist/*pyodide_2025*.whl
    EMSDK=$PWD/build/emsdk
    VENV=$PWD/build/pyodide-venv
    export PATH="$VENV/bin:$HOME/.cargo/bin:$EMSDK/upstream/emscripten:$PATH"
    export EMSDK EM_CONFIG=$EMSDK/.emscripten
    # No RUSTFLAGS: rustc removed -Zemscripten-wasm-eh (wasm EH is now the
    # unconditional default on Emscripten). USE_LEGACY_PLATFORM keeps the
    # pre-PEP-783 pyodide_* platform tag that the 0.29 runtime requires.
    export RUSTUP_TOOLCHAIN=nightly USE_LEGACY_PLATFORM=1
    export CARGO_BUILD_TARGET=wasm32-unknown-emscripten
    pyodide build --exports whole_archive
    _rc=$?
    mv "$_bak"/*.so src/libibt/ 2>/dev/null || true
    rm -rf "$_bak"
    [ $_rc -eq 0 ] && echo "Wheel built:" && ls dist/*wasm32*.whl
    exit $_rc

# Build and test with Pyodide 0.29 (wasm32-unknown-emscripten)
pyodide-test: pyodide-build pyodide-npm-setup
    #!/usr/bin/env bash
    set -euo pipefail
    node scripts/run_pyodide_tests.mjs --dist-dir=./dist --pyodide-version=0.29.3
    node scripts/run_pyodide_tests_idbfs.mjs --dist-dir=./dist --pyodide-version=0.29.3

# --- Pyodide 314 (Python 3.14, PEP 783 pyemscripten_2026_0 -> PyPI) ---
# Unlike 0.29 this builds on STABLE Rust with the rustflags the xbuildenv
# reports, and uses plain emcc/wasm-opt: the shims exist for older Emscripten
# toolchains and would strip flags Emscripten 5.0.3 requires.

# Toolchain for Pyodide 314 (Python 3.14 host, Emscripten 5.0.3)
emsdk-setup-314:
    #!/usr/bin/env bash
    set -euo pipefail
    VENV=build/pyodide-venv-314
    mkdir -p build
    [ -d "$VENV" ] || uv venv --seed --python 3.14 "$VENV"
    uv pip install --python "$VENV/bin/python" pyodide-build=={{ _pyodide_build_version }}
    source "$VENV/bin/activate"
    pyodide xbuildenv install 314.0.4
    # The xbuildenv names an exact toolchain (e.g. 1.93.0). rustup treats that
    # as its own toolchain, so the wasm target must be installed for it by name
    # -- having it on `stable` is not enough even when stable is that version.
    RUST_TOOLCHAIN=$(pyodide config get rust_toolchain)
    rustup toolchain install "$RUST_TOOLCHAIN" --profile minimal --target wasm32-unknown-emscripten --component rust-src
    EMSDK_VERSION=$(pyodide config get emscripten_version)
    echo "Emscripten version: $EMSDK_VERSION"
    [ -d build/emsdk-314/.git ] || git clone https://github.com/emscripten-core/emsdk.git build/emsdk-314
    cd build/emsdk-314 && git config core.autocrlf false && git checkout -- .
    ./emsdk install "$EMSDK_VERSION"
    ./emsdk activate "$EMSDK_VERSION"

# Install the Pyodide 314 npm package
pyodide-npm-setup-314:
    npm install pyodide-314@npm:pyodide@314.0.4

# Build the Pyodide 314 wheel (pyemscripten_2026_0, publishable to PyPI)
pyodide-build-314: emsdk-setup-314
    #!/usr/bin/env bash
    set -euo pipefail
    _bak=/tmp/_libibt_native_$$ && mkdir -p "$_bak"
    find src/libibt -maxdepth 1 -name '*linux-gnu.so' -exec mv {} "$_bak/" \; 2>/dev/null || true
    rm -f dist/*pyemscripten_2026*.whl
    EMSDK=$PWD/build/emsdk-314
    VENV=$PWD/build/pyodide-venv-314
    export PATH="$VENV/bin:$HOME/.cargo/bin:$EMSDK/upstream/emscripten:$PATH"
    export EMSDK EM_CONFIG=$EMSDK/.emscripten
    export RUSTUP_TOOLCHAIN=$("$VENV/bin/pyodide" config get rust_toolchain)
    export RUSTFLAGS=$("$VENV/bin/pyodide" config get rustflags)
    export CARGO_BUILD_TARGET=wasm32-unknown-emscripten
    pyodide build --exports whole_archive
    _rc=$?
    mv "$_bak"/*.so src/libibt/ 2>/dev/null || true
    rm -rf "$_bak"
    [ $_rc -eq 0 ] && echo "Wheel built:" && ls dist/*wasm32*.whl
    exit $_rc

# Build and test with Pyodide 314
pyodide-test-314: pyodide-build-314 pyodide-npm-setup-314
    #!/usr/bin/env bash
    set -euo pipefail
    node scripts/run_pyodide_tests.mjs --dist-dir=./dist --pyodide-version=314.0.4
    node scripts/run_pyodide_tests_idbfs.mjs --dist-dir=./dist --pyodide-version=314.0.4
