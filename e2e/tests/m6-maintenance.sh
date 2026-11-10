#!/usr/bin/env bash
# M6: cdcell maintains its own tables (design §11.3): metadata cleanup
# property set, snapshots expired down to a retained count, data intact.
source "$(dirname "$0")/lib.sh"
export SHOP_PREFIX="m6t-$(date +%s)"
fresh; start_cdcell; sleep 5
for _ in 1 2 3 4 5; do ./scripts/load.sh 40 4 3 >/dev/null; sleep 12; done   # several commits
sleep 15
P=$(curl -s "http://127.0.0.1:8281/catalog/v1/config?warehouse=cdc" | python3 -I -c 'import json,sys;print(json.load(sys.stdin)["defaults"]["prefix"])')
snaps() { curl -s "http://127.0.0.1:8281/catalog/v1/$P/namespaces/cdcell%1Fshop%1Fshop/tables/orders" | python3 -I -c 'import json,sys; m=json.load(sys.stdin)["metadata"]; print(len(m.get("snapshots",[])), m.get("properties",{}).get("write.metadata.delete-after-commit.enabled","unset"))'; }
read -r before prop <<<"$(snaps)"
[ "$prop" = "true" ] || fail "write.metadata.delete-after-commit.enabled is $prop"
[ "$before" -gt 3 ] || fail "need >3 snapshots to test expiry, have $before"
docker run --rm --network "$NET" -v "$TARGET_VOL:/t" celld-cdc/rust-dev:1.98.1 "$CDCELL_BIN" maintain --retain-last 2 --older-than-s 0 \
  || fail "cdcell maintain failed"
read -r after _ <<<"$(snaps)"
[ "$after" -le 2 ] || fail "snapshots after maintain: $after (want <= 2, had $before)"
python3 -I tools/verify_type1.py --shops $(seq -f "$SHOP_PREFIX-%02g" 1 3) || fail "data changed by maintenance"
echo "m6-maintenance: PASS ($before -> $after snapshots)"
