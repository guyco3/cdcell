#!/usr/bin/env bash
# Run cargo in a container (the host has no C toolchain): celld's Rust
# version plus clippy/rustfmt (scripts/rust-dev.Dockerfile, built on first
# use). Mounts ~/dev so the celld path dependency (../celld/crates/ltx)
# resolves. Usage: scripts/cargo.sh test
# RUST_NET=cdcell-e2e_fleet joins the e2e network (to run against the stack).
set -euo pipefail
DEV="$(cd "$(dirname "$0")/../.." && pwd)"
IMAGE="${RUST_IMAGE:-cdcell/rust-dev:1.98.1}"
if [ -z "${RUST_IMAGE:-}" ] && ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  docker build -q -t "$IMAGE" -f "$(dirname "$0")/rust-dev.Dockerfile" "$(dirname "$0")" >/dev/null
fi
exec docker run --rm -i \
  -v "$DEV:/work" -w /work/cdcell \
  -v cdcell-cargo-registry:/usr/local/cargo/registry \
  -v cdcell-target:/work/cdcell/target \
  -e CARGO_TERM_COLOR=never ${CDCELL_TEST_LOG:+-e CDCELL_TEST_LOG="$CDCELL_TEST_LOG"} \
  ${RUST_NET:+--network "$RUST_NET"} \
  "$IMAGE" cargo "$@"
