#!/usr/bin/env bash
# M2 gate: single-node celld → cdcell → Iceberg; ClickHouse type-1 == dump.
#  1. stop celld-b/c (celld-a alone uploads per-cell L0 files)
#  2. fresh JetStream consumer, fresh output tables
#  3. run cdcell (debug build from scripts/cargo.sh build -p cdcell)
#  4. load N new shops, wait for commits, verify every shop
# Needs the stack up (up.sh, cdc-up.sh). Run under caffeinate -i.
set -euo pipefail
cd "$(dirname "$0")/.."
NET=cdcell-e2e_fleet
RUN="m2-$(date +%s)"
SHOPS=${SHOPS:-5}; OPS=${OPS:-60}
nats() { docker run --rm --network "$NET" natsio/nats-box:latest nats -s nats://nats:4222 "$@"; }

nats consumer rm CELLD cdcell -f >/dev/null 2>&1 || true
# clean, constant starting state: empty celld fleet, stream, cdcell state, output
( source tests/lib.sh; fresh )
docker compose stop celld-b celld-c >/dev/null   # single node from here
docker rm -f cdcell-run >/dev/null 2>&1 || true
docker run -d --name cdcell-run --network "$NET" -p 9464:9464 -v cdcell-target:/t \
  -e RUST_LOG=info,iceberg=warn -e CDCELL_COMMIT_INTERVAL_S=10 -e CDCELL_GAP_TIMEOUT_S=15 \
  cdcell/rust-dev:1.98.1 /t/debug/cdcell run >/dev/null
sleep 5

names=()
for i in $(seq -w 1 "$SHOPS"); do
  s="$RUN-$i"; names+=("$s")
  seq 1 "$OPS" | xargs -P 4 -I{} curl -s -m 30 -o /dev/null "http://127.0.0.1:8180/?shop=$s&op=random"
done
echo "loaded ${#names[@]} shops x $OPS ops; waiting for commits"
( source tests/lib.sh; wait_caught_up )
docker logs cdcell-run 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep "committed batch" | tail -2 | cut -c1-200
python3 -I tools/verify_type1.py --shops "${names[@]}" && echo "M2: PASS"
