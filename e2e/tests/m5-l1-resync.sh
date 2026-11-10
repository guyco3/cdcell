#!/usr/bin/env bash
# M5: committed point folded away. cdcell commits a shop below TXID 256 and
# stops; while it is down (notifications lost) the shop passes celld's L1
# fold at 256 TXIDs and the bundles in between are GC'd, so the only image
# of the shop is an L1 1-256+ that ends beyond the committed point. On
# restart cdcell must resync from that image (net change incl. deletes from
# its own change log) instead of waiting forever. Then type-1 == dump.
source "$(dirname "$0")/lib.sh"
export SHOP_PREFIX="l1r-$(date +%s)"
SHOP="$SHOP_PREFIX-01"
fresh
start_cdcell
./scripts/load.sh 40 2 1 >/dev/null
wait_caught_up
stop_cdcell
nats stream purge CELLD -f >/dev/null 2>&1 || true   # changes while down are lost
echo "== drive $SHOP past the L1 fold while cdcell is down"
./scripts/load.sh 320 4 1 >/dev/null
sleep 60                                              # bundle GC
cell=$(mc ls "e2e/celld/cells/" | awk '{print $NF}' | tr -d / | head -1)
echo "   celld objects: $(mc ls -r "e2e/celld/cells/$cell/ltx/" | awk '{print $NF}' | tr '\n' ' ')"
start_cdcell
./scripts/load.sh 5 1 1 >/dev/null                    # wake notifications
wait_caught_up
docker logs cdcell-run 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -E "resynced|rebuilt" | cut -c1-220
docker logs cdcell-run 2>&1 | grep -q "resynced from a later full image" || fail "no resync from a later image (scenario not reproduced?)"
python3 -I tools/verify_type1.py --shops "$SHOP" --allow-dups && echo "m5-l1-resync: PASS"
