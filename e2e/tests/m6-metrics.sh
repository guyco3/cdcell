#!/usr/bin/env bash
# M6: cdcell exposes the design §12 metrics in Prometheus text format.
source "$(dirname "$0")/lib.sh"
export SHOP_PREFIX="m6m-$(date +%s)"
fresh; start_cdcell; sleep 5
./scripts/load.sh 150 6 4 >/dev/null; wait_caught_up
body=$(curl -sf http://127.0.0.1:9464/metrics) || fail "no metrics endpoint on :9464"
for m in cdcell_events_total cdcell_commits_total cdcell_commit_seconds_sum cdcell_units_total \
         cdcell_watermark_age_seconds_max cdcell_watermark_age_seconds_p99 cdcell_open_gaps cdcell_parked_cells cdcell_parked_age_seconds_max \
         cdcell_phantoms_dropped_total cdcell_duplicates_dropped_total cdcell_not_found_total \
         cdcell_decode_errors_total cdcell_resyncs_total cdcell_rebuilds_total cdcell_reconciled_bundles_total \
         'cdcell_s3_requests_total{op="get"}' 'cdcell_s3_requests_total{op="list"}' 'cdcell_s3_requests_total{op="put"}'; do
  grep -q "^$m " <<<"$body" || fail "missing metric $m"
done
[ "$(metric cdcell_events_total)" -gt 0 ] || fail "no events counted"
[ "$(metric cdcell_commits_total)" -gt 0 ] || fail "no commits counted"
age=$(metric cdcell_watermark_age_seconds_max); python3 -c "import sys; sys.exit(0 if float('$age') < 120 else 1)" || fail "watermark age $age s"
echo "m6-metrics: PASS"
