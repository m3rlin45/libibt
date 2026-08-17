#!/bin/bash
# Wrapper for wasm-opt that filters flags unsupported by older versions.
#
# Rust nightly emits --enable-bulk-memory-opt and --enable-call-indirect-overlong
# which older wasm-opt builds (pre-PEP-783 Pyodide toolchains) don't support.
# This wrapper strips those flags before delegating to the real wasm-opt binary.

args=()
for arg in "$@"; do
    case "$arg" in
        --enable-bulk-memory-opt|--enable-call-indirect-overlong)
            ;; # skip unsupported flags
        *)
            args+=("$arg")
            ;;
    esac
done
exec "$(dirname "$0")/wasm-opt.real" "${args[@]}"
