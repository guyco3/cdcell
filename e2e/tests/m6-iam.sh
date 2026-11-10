#!/usr/bin/env bash
# M6: least-privilege read policy for the celld bucket (design §12).
# cdcell runs with only these credentials; secrets and app data are denied.
source "$(dirname "$0")/lib.sh"
POLICY=../deploy/iam/celld-read-policy.json
[ -f "$POLICY" ] || fail "policy file $POLICY missing"
export SHOP_PREFIX="m6i-$(date +%s)"
fresh
./scripts/load.sh 20 2 1 >/dev/null   # some celld objects to read
mc admin policy create e2e cdcell-read "$POLICY" >/dev/null 2>&1 || mc admin policy create e2e cdcell-read "$POLICY" >/dev/null
mc admin user add e2e cdcellreader cdcellreader-secret >/dev/null
mc admin policy attach e2e cdcell-read --user cdcellreader >/dev/null 2>&1 || true
mc alias set e2e-ro http://127.0.0.1:9100 cdcellreader cdcellreader-secret >/dev/null
# fleet mode: a fresh shop has own.json and bundle rows, per-cell .ltx only after compaction
cell=$( (mc ls e2e/celld/cells/ || true) | awk '{print $NF}' | tr -d / | head -1)
[ -n "$cell" ] || fail "no cells after seed load"
mc cat "e2e-ro/celld/cells/$cell/own.json" >/dev/null 2>&1 || fail "reader cannot GET cells/*"
bundle=$( (mc ls -r e2e/celld/log/ || true) | awk '/\.ltxb$/ {print $NF}' | head -1)
[ -n "$bundle" ] || fail "no bundles after seed load"
mc cat "e2e-ro/celld/log/$bundle" >/dev/null 2>&1 || fail "reader cannot GET log/*/bundle"
mc cat e2e-ro/celld/deploy/current.json >/dev/null 2>&1 || fail "reader cannot GET deploy/current.json"
mc ls e2e-ro/celld/log/ >/dev/null 2>&1 || fail "reader cannot LIST log/"
mc cat e2e-ro/celld/fleet/peer-auth.json >/dev/null 2>&1 && fail "reader CAN read fleet/peer-auth.json"
echo secret | mc pipe e2e/celld/r2/assets/test.txt >/dev/null
mc cat e2e-ro/celld/r2/assets/test.txt >/dev/null 2>&1 && fail "reader CAN read r2/ app data"
mc cat "e2e-ro/celld/deploy/shop/$( (mc ls e2e/celld/deploy/shop/ || true) | awk '/\/$/ {print $NF}' | head -1)index.js" >/dev/null 2>&1 && fail "reader CAN read Worker code"
# and cdcell works end to end with only these credentials
fresh; start_cdcell -e CDCELL_SOURCE_ACCESS_KEY=cdcellreader -e CDCELL_SOURCE_SECRET_KEY=cdcellreader-secret; sleep 5
./scripts/load.sh 60 4 2 >/dev/null; wait_caught_up
python3 -I tools/verify_type1.py --shops $(seq -f "$SHOP_PREFIX-%02g" 1 2) || fail "cdcell with least-privilege credentials"
echo "m6-iam: PASS"
