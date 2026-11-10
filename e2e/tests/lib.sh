# Shared helpers for e2e/tests/*.sh (source it).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NET=cdcell-e2e_fleet
CDCELL_BIN=${CDCELL_BIN:-/t/debug/cdcell}
TARGET_VOL=${TARGET_VOL:-cdcell-target}
nats() { docker run --rm --network "$NET" natsio/nats-box:latest nats -s nats://nats:4222 "$@"; }
settle() { for _ in $(seq 1 40); do n=$(python3 -I tools/inspect.py leases | grep -c '"log_state": "open"' || true); [ "$n" -ge 3 ] && return; sleep 2; done; }
# start_cdcell [extra docker run args...]: metrics published on :9464
start_cdcell() {
  docker rm -f cdcell-run >/dev/null 2>&1 || true
  docker run -d --name cdcell-run --network "$NET" -p 9464:9464 -v "$TARGET_VOL:/t" \
    -e RUST_LOG=info,iceberg=warn -e CDCELL_COMMIT_INTERVAL_S=10 -e CDCELL_GAP_TIMEOUT_S=15 -e CDCELL_RECONCILE_S=30 \
    "$@" celld-cdc/rust-dev:1.98.1 "$CDCELL_BIN" run >/dev/null
}
stop_cdcell() { docker rm -f cdcell-run >/dev/null 2>&1 || true; }
# Wipe celld's runtime state (cells, logs, leases, wake/drain records) so
# every test starts from the same empty fleet. Keeps configuration: the
# deployed Worker code (deploy/), the fleet peer secret (fleet/) and r2/.
reset_celld() {
  docker compose stop celld-a celld-b celld-c >/dev/null 2>&1
  for p in cells log nodes wake drain; do
    mc rm --recursive --force "e2e/celld/$p/" >/dev/null 2>&1 || true
  done
  docker compose start celld-a celld-b celld-c >/dev/null 2>&1; settle
}
# Clean, constant starting state for a test: empty celld fleet, empty
# notification stream, no cdcell state, no output tables.
fresh() {
  stop_cdcell
  reset_celld
  nats consumer rm CELLD cdcell -f >/dev/null 2>&1 || true
  nats stream purge CELLD -f >/dev/null 2>&1 || true   # each test sees only its own notifications
  mc rm --recursive --force cdc/warehouse/_cdcell/ >/dev/null 2>&1 || true
  drop_output
}
# Drop every table under the cdcell.* namespaces: a fresh cdcell deployment
# starts with fresh output (otherwise re-bootstrapped history duplicates).
drop_output() {
  python3 -I - <<'PY'
import json, urllib.request
base = "http://127.0.0.1:8281/catalog/v1"
prefix = json.load(urllib.request.urlopen(f"{base}/config?warehouse=cdc"))["defaults"]["prefix"]
def get(path):
    return json.load(urllib.request.urlopen(f"{base}/{prefix}/{path}"))
def walk(ns):
    enc = "%1F".join(ns)
    for t in get(f"namespaces/{enc}/tables").get("identifiers", []):
        req = urllib.request.Request(f"{base}/{prefix}/namespaces/{enc}/tables/{t['name']}?purgeRequested=true", method="DELETE")
        urllib.request.urlopen(req)
    for child in get(f"namespaces?parent={enc}").get("namespaces", []):
        walk(child)
try:
    walk(["cdcell"])
except urllib.error.HTTPError as e:
    if e.code != 404:
        raise
PY
}
metric() { { curl -sf http://127.0.0.1:9464/metrics || true; } | awk -v m="$1" '$1==m {print $2}' | tail -1; }
fail() { echo "FAIL: $*"; exit 1; }
# Wait until cdcell has caught up: watermark under 30 s and no open gaps
# (bounded, default 300 s). Replaces fixed sleeps before verification.
wait_caught_up() {
  local limit=${1:-300} t=0
  sleep 15
  while [ "$t" -lt "$limit" ]; do
    a=$(metric cdcell_watermark_age_seconds_max); g=$(metric cdcell_open_gaps)
    python3 -c "import sys; sys.exit(0 if float('${a:-999}') < 30 and float('${g:-1}') == 0 else 1)" && return 0
    sleep 5; t=$((t + 5))
  done
  echo "   (not caught up after ${limit}s: watermark ${a:-?} s, gaps ${g:-?})"
}

