#!/usr/bin/env bash
# M4 gate: epochs and phantoms under faults. Fresh shops get load while the
# busiest node is paused, stalled, turned into a zombie, and killed; a final
# write to every shop lets each new epoch's opener resolve parked suspects.
# Then ClickHouse type-1 == dump for every shop.
#   m4-gate.sh [SCENARIOS="pause stall zombie kill"]
# Needs the stack up (up.sh, cdc-up.sh) and a debug build. caffeinate -i.
set -euo pipefail
cd "$(dirname "$0")/.."
NET=cdcell-e2e_fleet
export SHOP_PREFIX="m4-$(date +%s)"
SCENARIOS=${1:-pause stall zombie kill}
nats() { docker run --rm --network "$NET" natsio/nats-box:latest nats -s nats://nats:4222 "$@"; }
busiest() { python3 -I tools/inspect.py leases | python3 -I -c 'import json,sys; ls=[json.loads(l) for l in sys.stdin]; print(max(ls, key=lambda l: l.get("owned_cells") or 0)["node"])'; }
settle() { for _ in $(seq 1 40); do n=$(python3 -I tools/inspect.py leases | grep -c '"log_state": "open"' || true); [ "$n" -ge 3 ] && return; sleep 2; done; }

docker compose start celld-a celld-b celld-c >/dev/null
settle
nats consumer rm CELLD cdcell -f >/dev/null 2>&1 || true
# clean, constant starting state: empty celld fleet, stream, cdcell state, output
( source tests/lib.sh; fresh )
docker rm -f cdcell-run >/dev/null 2>&1 || true
docker run -d --name cdcell-run --network "$NET" -p 9464:9464 -v cdcell-target:/t \
  -e RUST_LOG=info,iceberg=warn -e CDCELL_COMMIT_INTERVAL_S=10 -e CDCELL_GAP_TIMEOUT_S=15 \
  cdcell/rust-dev:1.98.1 /t/debug/cdcell run >/dev/null
sleep 5
echo "== seed load ($SHOP_PREFIX-01..10)"; ./scripts/load.sh 200 6 10

for s in $SCENARIOS; do
  echo "== scenario: $s"
  case "$s" in
    pause|stall|zombie) ./scripts/zombie-test.sh "$s" 2>&1 | grep -E "^== |SUSPECT|CONFIRMED.*VIOLATION|summary" | tail -6 ;;
    kill)
      n=$(busiest); echo "   docker kill $n"
      ( ./scripts/load.sh 120 6 10 >/dev/null ) & L=$!
      sleep 5; docker compose kill "$n" >/dev/null; sleep 20; docker compose start "$n" >/dev/null; wait $L ;;
  esac
  settle
done

echo "== final write to every shop (new epochs emit their openers)"
./scripts/load.sh 40 4 10
( source tests/lib.sh; wait_caught_up )
docker logs cdcell-run 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -E "committed batch|paged|WARN|ERROR" | tail -4 | cut -c1-280
python3 -I tools/verify_type1.py --shops $(seq -f "$SHOP_PREFIX-%02g" 1 10) && echo "M4: PASS"
