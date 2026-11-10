#!/usr/bin/env bash
# Rev 5: schema evolution across a celld deployment (reproduces the schema
# experiment). Shops A-D on shop v1; deploy shop-v2 (constructor adds
# orders.note and table tags) while B keeps writing; touch A and C, create E,
# leave D alone. Pass: type-1 == dump for all five; D's orders rows stay on
# __schema_version 1; A, B, C reach 2; E is created on 2.
source "$(dirname "$0")/lib.sh"
deploy() { ( cd "worker/$1" && AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH" \
  celld deploy . --bucket s3://celld --endpoint http://127.0.0.1:9100 --region us-east-1 >/dev/null 2>&1 ) || fail "deploy $1"; }
shop_req() { curl -s -m 30 -o /dev/null "http://127.0.0.1:8180/?shop=$1&op=${2:-random}"; }
P="r5-$(date +%s)"; A=$P-a; B=$P-b; C=$P-c; D=$P-d; E=$P-e
trap 'deploy shop' EXIT                                   # always restore v1
deploy shop; sleep 35
fresh; start_cdcell; sleep 5
for s in $A $B $C $D; do for _ in $(seq 1 25); do shop_req "$s"; done; done
( for _ in $(seq 1 60); do shop_req "$B"; sleep 1; done ) & W=$!
deploy shop-v2; sleep 40                                   # nodes adopt within a poll
wait $W
for s in $A $C; do for _ in $(seq 1 10); do shop_req "$s"; done; done
for _ in $(seq 1 20); do shop_req "$E"; done
for s in $A $B $C $E; do shop_req "$s" note; done
sleep 45
python3 -I tools/verify_type1.py --shops $A $B $C $D $E --schema-versions $A $B $C $D $E | tee /tmp/r5-schema.out || fail "type-1 after schema evolution"
grep -q "^SCHEMA $D \[1\]$" /tmp/r5-schema.out || fail "untouched cell D must stay on schema version 1"
for s in $A $B $C; do grep -qE "^SCHEMA $s \[1, 2\]$" /tmp/r5-schema.out || fail "$s must move from version 1 to 2"; done
grep -qE "^SCHEMA $E \[2\]$" /tmp/r5-schema.out || fail "new cell E must be on version 2"
echo "r5-schema-evolution: PASS"
