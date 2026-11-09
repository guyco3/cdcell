#!/usr/bin/env bash
# Run cargo inside the celld Rust dev image (the host has no C toolchain, and
# this matches celld's Rust version). Mounts ~/dev so the celld path
# dependency (../celld/crates/ltx) resolves. Usage: scripts/cargo.sh test
set -euo pipefail
DEV="$(cd "$(dirname "$0")/../.." && pwd)"
exec docker run --rm -i \
  -v "$DEV:/work" -w /work/cdcell \
  -v cdcell-cargo-registry:/usr/local/cargo/registry \
  -v cdcell-target:/work/cdcell/target \
  -e CARGO_TERM_COLOR=never \
  "${RUST_IMAGE:-celld-cdc/rust-dev:1.98.1}" cargo "$@"
