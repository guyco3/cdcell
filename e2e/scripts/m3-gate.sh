#!/usr/bin/env bash
# M3 gate: 3-node fleet (bundles, lease confirmation, rebalancing epochs) →
# cdcell → Iceberg; ClickHouse type-1 == dump and no duplicate events.
# Needs the stack up (up.sh, cdc-up.sh) and a debug build. caffeinate -i.
set -euo pipefail
cd "$(dirname "$0")/.."
NET=cdcell-e2e_fleet
RUN="m3-$(date +%s)"
SHOPS=${SHOPS:-6}; OPS=${OPS:-80}
nats() { docker run --rm --network "$NET" natsio/nats-box:latest nats -s nats://nats:4222 "$@"; }

docker compose start celld-a celld-b celld-c >/dev/null
for _ in $(seq 1 40); do   # wait until every node's log is open with an ensemble
  n=$(python3 -I tools/inspect.py leases | grep -c '"log_state": "open"' || true); [ "$n" -ge 3 ] && break; sleep 2
done
nats consumer rm CELLD cdcell -f >/dev/null 2>&1 || true
# clean, constant starting state: empty celld fleet, stream, cdcell state, output
( source tests/lib.sh; fresh )
docker rm -f cdcell-run >/dev/null 2>&1 || true
docker run -d --name cdcell-run --network "$NET" -p 9464:9464 -v cdcell-target:/t \
  -e RUST_LOG=info,iceberg=warn -e CDCELL_COMMIT_INTERVAL_S=10 -e CDCELL_GAP_TIMEOUT_S=15 \
  celld-cdc/rust-dev:1.98.1 /t/debug/cdcell run >/dev/null
sleep 5
names=()
for i in $(seq -w 1 "$SHOPS"); do names+=("$RUN-$i"); done
printf '%s\n' "${names[@]}" | while read -r s; do
  seq 1 "$OPS" | xargs -P 4 -I{} curl -s -m 30 -o /dev/null "http://127.0.0.1:8180/?shop=$s&op=random"
done
echo "loaded ${#names[@]} shops x $OPS ops through the load balancer; waiting for commits"
( source tests/lib.sh; wait_caught_up )
docker logs cdcell-run 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -E "committed batch|WARN|ERROR" | tail -3 | cut -c1-260
python3 -I tools/verify_type1.py --shops "${names[@]}" && echo "M3: PASS"
