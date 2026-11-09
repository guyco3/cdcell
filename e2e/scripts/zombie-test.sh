#!/usr/bin/env bash
# Make one celld node lose its lease mid-load, let the fleet recover it, then
# check cdcell's bundle-confirmation rule against what the fleet kept.
#
#   zombie-test.sh pause [SECONDS=25]   docker pause celld-c (process freeze)
#   zombie-test.sh zombie [SECONDS=22]  stall uploads, then freeze celld-c: an
#                                        in-flight bundle lands after recovery
#   zombie-test.sh stall [SECONDS=20]   delay celld-c's uploads to the bucket by
#                                        SECONDS in flight (toxiproxy). A bundle
#                                        PUT already sent lands after recovery:
#                                        the zombie case.
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=${1:-zombie}; SECS=${2:-}
# Target the node that owns the most cells right now (or NODE=celld-x).
NODE=${NODE:-$(python3 -I tools/inspect.py leases | python3 -I -c '
import json,sys
ls=[json.loads(l) for l in sys.stdin if l.strip()]
print(max(ls, key=lambda l: l.get("owned_cells") or 0)["node"])')}
PROXY="s3-$NODE"
echo "== target $NODE"
OUT=${OUT:-/tmp/cdcell-zombie}; mkdir -p "$OUT"
EVENTS="$OUT/events-$MODE-$(date +%s).jsonl"
toxi() { docker compose exec -T toxiproxy toxiproxy-cli "$@" >/dev/null; }

python3 -I tools/inspect.py watch -o "$EVENTS" --interval 0.5 2>/dev/null &
WATCH=$!
( while :; do ./scripts/load.sh 40 6 10 >/dev/null; done ) &
LOAD=$!
cleanup() {
  kill $WATCH $LOAD 2>/dev/null || true
  docker compose unpause "$NODE" >/dev/null 2>&1 || true
  toxi toxic remove -n stall "$PROXY" 2>/dev/null || true
}
trap cleanup EXIT

sleep 10
case "$MODE" in
  pause)
    SECS=${SECS:-25}
    echo "== $(date +%T) pause $NODE for ${SECS}s (lease TTL 10s)"
    docker compose pause "$NODE"; sleep "$SECS"; docker compose unpause "$NODE" ;;
  stall)
    SECS=${SECS:-20}
    echo "== $(date +%T) stall $NODE -> bucket uploads by ${SECS}s in flight"
    toxi toxic add -t latency -a "latency=$((SECS * 1000))" -u -n stall "$PROXY"
    sleep $((SECS + 5))
    toxi toxic remove -n stall "$PROXY" ;;
  zombie)
    # Stall uploads, let a bundle PUT enter the delayed link, then freeze the
    # node so its connection stays open: the PUT reaches the bucket ~SECS later,
    # after the fleet has recovered the node. A frozen-but-alive sender is what
    # a GC pause or VM stall looks like in production.
    SECS=${SECS:-22}
    echo "== $(date +%T) stall $NODE uploads by ${SECS}s, then freeze it"
    toxi toxic add -t latency -a "latency=$((SECS * 1000))" -u -n stall "$PROXY"
    sleep 3
    docker compose pause "$NODE"
    # Once the frozen node's lease has expired (TTL 10 s), touch every shop on
    # a surviving node directly: each takeover runs recovery right away, well
    # before the delayed PUT lands.
    sleep 13
    case "$NODE" in celld-a) SURVIVOR_PORT=8182 ;; *) SURVIVOR_PORT=8181 ;; esac
    echo "== $(date +%T) force takeovers via 127.0.0.1:$SURVIVOR_PORT"
    URL=http://127.0.0.1:$SURVIVOR_PORT ./scripts/load.sh 20 10 10 | tr '\n' ' '; echo
    sleep $((SECS - 10))
    toxi toxic remove -n stall "$PROXY"
    docker compose unpause "$NODE" ;;
  *) echo "mode must be pause, stall or zombie" >&2; exit 2 ;;
esac
echo "== $(date +%T) fault cleared"
sleep 25
kill $LOAD 2>/dev/null || true
sleep 8
kill $WATCH 2>/dev/null || true
trap - EXIT

echo "== $NODE: $(docker compose ps "$NODE" --format '{{.State}} {{.Status}}')"
docker compose logs --since 3m "$NODE" 2>&1 | grep -E "SELF-FENCE" | tail -2 | cut -c1-160 || true
echo
python3 -I tools/zombie_report.py "$EVENTS" "$NODE"
echo "events: $EVENTS"
