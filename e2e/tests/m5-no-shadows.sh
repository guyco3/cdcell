#!/usr/bin/env bash
# cdcell keeps no copy of celld state: no shadow images in the customer
# bucket. After a crash it rebuilds each cell from the celld bucket (design
# §10/§18.3), re-emitting at most a bounded window as synthetic events.
source "$(dirname "$0")/lib.sh"
export SHOP_PREFIX="m5n-$(date +%s)"
fresh; start_cdcell; sleep 5
./scripts/load.sh 200 6 6 >/dev/null; sleep 30          # several commits
n=$( (mc ls -r cdc/warehouse/_cdcell/shadows/ 2>/dev/null || true) | wc -l | tr -d ' ')
[ "$n" -eq 0 ] || fail "$n shadow images written to _cdcell/shadows/"
( ./scripts/load.sh 300 6 6 >/dev/null ) & L=$!
sleep 12; echo "   kill -9 cdcell"; stop_cdcell; sleep 3; start_cdcell
wait $L
./scripts/load.sh 30 4 6 >/dev/null; sleep 60
n=$( (mc ls -r cdc/warehouse/_cdcell/shadows/ 2>/dev/null || true) | wc -l | tr -d ' ')
[ "$n" -eq 0 ] || fail "$n shadow images written to _cdcell/shadows/ (after restart)"
python3 -I tools/verify_type1.py --shops $(seq -f "$SHOP_PREFIX-%02g" 1 6) || fail "after rebuild from celld"
echo "m5-no-shadows: PASS"
