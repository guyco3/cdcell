#!/usr/bin/env bash
# M5: committed epoch GC'd. cdcell commits a shop in epoch E and stops.
# While it is down (notifications retained: the queue keeps them, nothing is
# lost) the shop moves through several epochs and celld's epoch GC deletes
# E's prefix (celld logic/epoch_gc.rs: epochs below the restore chain's base
# are never read again). On restart cdcell's committed epoch no longer
# exists; it must resync from the current chain instead of blocking.
# Then type-1 == dump.
source "$(dirname "$0")/lib.sh"
export SHOP_PREFIX="egc-$(date +%s)"
SHOP="$SHOP_PREFIX-01"
fresh
# enable celld's epoch GC (opt-in) for this test; restore the default after
echo "== celld with CELLD_LTX_RETENTION_SECS=5"
CELLD_LTX_RETENTION_SECS=5 docker compose up -d celld-a celld-b celld-c >/dev/null 2>&1; settle
trap 'docker compose up -d celld-a celld-b celld-c >/dev/null 2>&1' EXIT
start_cdcell
./scripts/load.sh 40 2 1 >/dev/null
wait_caught_up
cell=$(mc ls e2e/celld/cells/ | awk '{print $NF}' | tr -d / | head -1)
e0=$(mc cat "e2e/celld/cells/$cell/own.json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["epoch"])')
echo "   committed in epoch $e0; stopping cdcell (notifications stay queued)"
stop_cdcell
for r in 1 2 3; do
  CELLD_LTX_RETENTION_SECS=5 docker compose restart celld-a celld-b celld-c >/dev/null 2>&1; settle
  ./scripts/load.sh 30 2 1 >/dev/null                # new epoch: activation + writes
done
gone=""
for _ in $(seq 1 100); do                             # owner GC pass: at most every 5 min
  mc ls "e2e/celld/cells/$cell/ltx/e$e0/" 2>/dev/null | grep -q . || { gone=1; break; }
  ./scripts/load.sh 5 1 1 >/dev/null; sleep 5
done
echo "   epochs now: $(mc ls "e2e/celld/cells/$cell/ltx/" | awk '{print $NF}' | tr '\n' ' ')own=$(mc cat "e2e/celld/cells/$cell/own.json")"
[ -n "$gone" ] || fail "epoch e$e0 was not GC'd (scenario not reproduced)"
start_cdcell
./scripts/load.sh 5 1 1 >/dev/null
wait_caught_up
docker logs cdcell-run 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -E "resynced|rebuilt|lagging cell" | tail -3 | cut -c1-240
docker logs cdcell-run 2>&1 | grep -q "resynced from a later full image" || fail "no resync after the committed epoch was GC'd"
verify_eventually --shops "$SHOP" --allow-dups && echo "m5-epoch-gc: PASS"
