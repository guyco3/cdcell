#!/usr/bin/env bash
# M5 gate (robustness), faults injected from outside cdcell:
#   crash   kill -9 cdcell 3 times under load, restarting it each time
#   lost    with cdcell stopped, delete ~25% of the stored notifications,
#           always including the last (tail loss)
#   outage  cdcell down 3 min under heavy load: celld folds to L1 and GCs the
#           bundles before cdcell reads them
# Pass: ClickHouse type-1 == dump for every shop, and no duplicate events.
#   m5-gate.sh [SCENARIOS="crash lost outage"]
# CDCELL_BIN picks the binary inside the cdcell-target volume (default the
# current debug build); TARGET_VOL picks the volume (to test an old build).
# Needs the stack up (up.sh, cdc-up.sh). Run under caffeinate -i.
set -euo pipefail
cd "$(dirname "$0")/.."
NET=cdcell-e2e_fleet
SCENARIOS=${1:-crash lost outage}
CDCELL_BIN=${CDCELL_BIN:-/t/debug/cdcell}
TARGET_VOL=${TARGET_VOL:-cdcell-target}
export SHOP_PREFIX="m5-$(date +%s)"
nats() { docker run --rm --network "$NET" natsio/nats-box:latest nats -s nats://nats:4222 "$@"; }
settle() { for _ in $(seq 1 40); do n=$(python3 -I tools/inspect.py leases | grep -c '"log_state": "open"' || true); [ "$n" -ge 3 ] && return; sleep 2; done; }
start_cdcell() {
  docker rm -f cdcell-run >/dev/null 2>&1 || true
  docker run -d --name cdcell-run --network "$NET" -p 9464:9464 -v "$TARGET_VOL:/t" \
    -e RUST_LOG=info,iceberg=warn -e CDCELL_COMMIT_INTERVAL_S=10 -e CDCELL_GAP_TIMEOUT_S=15 -e CDCELL_RECONCILE_S=30 \
    celld-cdc/rust-dev:1.98.1 "$CDCELL_BIN" run >/dev/null
}
stop_cdcell() { docker kill cdcell-run >/dev/null 2>&1 || true; docker rm -f cdcell-run >/dev/null 2>&1 || true; }

# fresh start: fleet up, new consumer, empty cdcell state
docker compose start celld-a celld-b celld-c >/dev/null; settle
stop_cdcell
nats consumer rm CELLD cdcell -f >/dev/null 2>&1 || true
# clean, constant starting state: empty celld fleet, stream, cdcell state, output
( source tests/lib.sh; fresh )
mc rm --recursive --force cdc/warehouse/_cdcell/ >/dev/null 2>&1 || true
start_cdcell; sleep 5
echo "== seed load ($SHOP_PREFIX-01..06)"; ./scripts/load.sh 120 6 6

for s in $SCENARIOS; do
  echo "== scenario: $s"
  case "$s" in
    crash)
      ( ./scripts/load.sh 300 6 6 >/dev/null ) & L=$!
      for i in 1 2 3; do sleep $((7 + i * 3)); echo "   kill -9 cdcell ($i)"; stop_cdcell; sleep 2; start_cdcell; done
      wait $L ;;
    lost)
      stop_cdcell
      first=$(nats stream info CELLD -j | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["state"]["last_seq"])')
      ./scripts/load.sh 200 6 6 >/dev/null; sleep 4
      last=$(nats stream info CELLD -j | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["state"]["last_seq"])')
      # fleet mode yields ~1 bundle notification per node per 2 s, so drop
      # ~25% and always the last one (tail loss: no later unit reveals it)
      n=0; for seq in $(seq $((first + 1)) "$last"); do
        if [ "$seq" -eq "$last" ] || [ $((seq * 2654435761 % 100)) -lt 25 ]; then
          nats stream rmm CELLD "$seq" -f >/dev/null 2>&1 && n=$((n + 1))
        fi
      done
      echo "   deleted $n of $((last - first)) notifications (incl. the last)"
      [ "$n" -ge 1 ] || { echo "lost scenario injected no loss"; exit 1; }
      start_cdcell ;;
    outage)
      stop_cdcell
      ./scripts/load.sh 2000 8 6 >/dev/null        # >256 TXIDs per shop: L1 folds
      echo "   cdcell down; waiting for bundle GC"; sleep 180
      start_cdcell ;;
  esac
done

echo "== final write to every shop"; ./scripts/load.sh 30 4 6
( source tests/lib.sh; wait_caught_up )
docker logs cdcell-run 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -E "committed batch|recovered|rolling|reconcil|gap repair|ERROR|Error" | tail -6 | cut -c1-240
python3 -I tools/verify_type1.py --shops $(seq -f "$SHOP_PREFIX-%02g" 1 6) && echo "M5: PASS"
