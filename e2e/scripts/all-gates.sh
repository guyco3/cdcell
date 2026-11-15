#!/usr/bin/env bash
# Run every gate and test in sequence; print one PASS/FAIL line each.
#   all-gates.sh [names...]   (default: all)
cd "$(dirname "$0")/.."
ALL="scripts/m2-gate.sh scripts/m3-gate.sh scripts/m4-gate.sh scripts/m5-gate.sh tests/m5-no-shadows.sh tests/m5-l1-resync.sh tests/m5-epoch-gc.sh tests/m5-bootstrap.sh tests/r5-schema-evolution.sh tests/m6-metrics.sh tests/m6-maintenance.sh tests/m6-iam.sh"
LOG=${LOG:-/tmp/cdcell-gates}; mkdir -p "$LOG"
fails=0
for t in ${@:-$ALL}; do
  name=$(basename "$t" .sh); start=$(date +%s)
  if ./"$t" >"$LOG/$name.log" 2>&1 && grep -qE ": PASS( |$)" "$LOG/$name.log"; then r=PASS; else r=FAIL; fails=$((fails + 1)); fi
  echo "$r $name ($(( $(date +%s) - start ))s)  log: $LOG/$name.log"
done
docker compose start celld-a celld-b celld-c >/dev/null 2>&1   # m2 stops two nodes
exit $fails
