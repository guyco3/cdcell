#!/usr/bin/env bash
# M6 gate: soak at ~10 celld objects/s for SOAK_MIN minutes (default 60).
#  - lag: cdcell_watermark_age_seconds_p99 < 120 s at every sample once the
#    initial catch-up of pre-existing cells is done (watermark < 60 s)
#  - cost: cdcell GETs per celld object within ±20% of the model (design §13:
#    1 GET per notified object + confirmation reads amortized per round)
#  - correctness: type-1 == dump at the end
source "$(dirname "$0")/lib.sh"
SOAK_MIN=${SOAK_MIN:-60}
export SHOP_PREFIX="m6s-$(date +%s)"
fresh; start_cdcell; sleep 5
first=$(nats stream info CELLD -j | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["state"]["last_seq"])')
# Catch-up of the fleet's pre-existing cells is not steady state: start the
# soak clock once the watermark is under 60 s (bounded at 10 min).
for _ in $(seq 1 60); do
  ./scripts/load.sh 20 4 10 >/dev/null
  a=$(metric cdcell_watermark_age_seconds_max); python3 -c "import sys; sys.exit(0 if float('${a:-999}') < 60 else 1)" && break
done
echo "   caught up; soak clock starts"
start=$(date +%s); end=$(( start + SOAK_MIN * 60 )); worst=0
while [ "$(date +%s)" -lt "$end" ]; do
  ./scripts/load.sh 60 4 10 >/dev/null          # ~fleet bundles + per-cell objects
  p99=$(metric cdcell_watermark_age_seconds_p99 || echo 999)
  worst=$(python3 -c "print(max(float('$worst'), float('${p99:-999}')))")
done
sleep 40
last=$(nats stream info CELLD -j | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["state"]["last_seq"])')
# objects that need a GET: .ltx / .ltxb notifications (own.json needs none),
# classified from the stream itself, not from cdcell's counters
objects=$(docker run --rm --network "$NET" natsio/nats-box:latest sh -c \
  "for s in \$(seq $((first + 1)) $last); do nats -s nats://nats:4222 stream get CELLD \$s -j 2>/dev/null; echo; done" \
  | python3 -I -c '
import base64, json, sys
buf, n = "", 0
for line in sys.stdin:
    buf += line
    if line.strip() == "}":
        try:
            ev = json.loads(base64.b64decode(json.loads(buf)["data"]))
            n += ev.get("Key", "").endswith((".ltx", ".ltxb"))
        except Exception:
            pass
        buf = ""
print(n)')
gets=$(metric 'cdcell_s3_requests_total{op="get"}'); rounds=$(metric cdcell_confirm_reads_total)
[ -n "$gets" ] && [ -n "$rounds" ] || fail "request metrics missing (cdcell_s3_requests_total, cdcell_confirm_reads_total)"
python3 - "$objects" "$gets" "$rounds" "$worst" <<'PY' || fail "soak budget"
import sys
objects, gets, confirm, worst = int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
model = objects + confirm          # 1 GET per object + the confirmation reads actually needed
ratio = gets / model if model else 0
print(f"objects={objects} gets={gets:.0f} model={model:.0f} ratio={ratio:.2f} worst_lag_p99={worst:.1f}s")
sys.exit(0 if 0.8 <= ratio <= 1.2 and worst < 120 else 1)
PY
python3 -I tools/verify_type1.py --shops $(seq -f "$SHOP_PREFIX-%02g" 1 10) || fail "type-1 after soak"
echo "m6-soak: PASS"
