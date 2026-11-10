#!/usr/bin/env bash
# M5 (design §10): a fresh cdcell meets cells that already have long
# histories (several epochs, bundles GC'd, per-cell L1 objects). It must
# bootstrap them from their current epoch's base and then stream; type-1 ==
# dump after new writes. The history is built here, with cdcell off.
source "$(dirname "$0")/lib.sh"
export SHOP_PREFIX="m5b-$(date +%s)"
SHOPS=$(seq -f "$SHOP_PREFIX-%02g" 1 3)
fresh
echo "== build history with cdcell off: 3 rounds of load + fleet restart (new epochs)"
for r in 1 2 3; do
  ./scripts/load.sh 150 4 3 >/dev/null
  docker compose restart celld-a celld-b celld-c >/dev/null 2>&1; settle
done
sleep 60                                              # bundle GC
nats stream purge CELLD -f >/dev/null 2>&1 || true   # cdcell never saw any of it
c=$(mc ls e2e/celld/cells/ | awk "{print \$NF}" | tr -d / | head -1)
echo "   e.g. $c: $(mc ls -r "e2e/celld/cells/$c/ltx/" | awk '{print $NF}' | cut -d/ -f1-2 | sort -u | tr '\n' ' ')"
start_cdcell; sleep 5
for s in $SHOPS; do seq 1 20 | xargs -P 4 -I{} curl -s -m 30 -o /dev/null "http://127.0.0.1:8180/?shop=$s&op=random"; done
wait_caught_up
python3 -I tools/verify_type1.py --shops $SHOPS || fail "bootstrap of cells with history"
age=$(metric cdcell_watermark_age_seconds_max); python3 -c "import sys; sys.exit(0 if float('${age:-999}') < 120 else 1)" || fail "watermark age $age s"
echo "m5-bootstrap: PASS"
