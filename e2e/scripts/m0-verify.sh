#!/usr/bin/env bash
# M0 gate: the customer side works end to end with a hand-written Iceberg table.
#  1. Lakekeeper multi-table transactions are atomic
#  2. ClickHouse reads the table through Lakekeeper (DataLakeCatalog, vended creds)
#  3. the design's SCD type-1 query and the UInt128 ReplacingMergeTree version work
set -euo pipefail
cd "$(dirname "$0")/.."
NET=cdcell-e2e_fleet

docker volume create cdcell-e2e_pipcache >/dev/null
docker run --rm --network "$NET" -v "$PWD/m0:/m0:ro" -v cdcell-e2e_pipcache:/root/.cache/pip \
  python:3.12-slim sh -c 'pip install -q --disable-pip-version-check "pyiceberg[pyarrow]==0.10.0" 2>/dev/null; python -I /m0/verify_iceberg.py' \
  | tee /dev/stderr | grep -q "RESULT PASS"

ch() { curl -sf -u default:clickhouse "http://127.0.0.1:8123/?allow_database_iceberg=1" --data-binary "$1"; }
ch "DROP DATABASE IF EXISTS lake"
ch "CREATE DATABASE lake ENGINE = DataLakeCatalog('http://lakekeeper:8181/catalog') SETTINGS catalog_type = 'rest', warehouse = 'cdc', vended_credentials = true"
scd1=$(ch "SELECT _cell_id || ' ' || _key_json || ' ' || argMax(after, (_epoch, _txid)).status FROM (SELECT * FROM lake.\`cdcell.shop.shop.orders\` LIMIT 1 BY ce_source, ce_id) GROUP BY _cell_id, _key_json HAVING argMax(op, (_epoch, _txid)) != 'd' ORDER BY 1")
expected=$'Shop:aaaa [1] paid\nShop:bbbb [1] shipped'
[ "$scd1" = "$expected" ] && echo "SCD1 via Lakekeeper: PASS" || { echo "SCD1 FAIL: $scd1"; exit 1; }

ch "DROP TABLE IF EXISTS m0_rmt"
ch "CREATE TABLE m0_rmt (k String, v String, ver UInt128, is_deleted UInt8) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY k"
ch "INSERT INTO m0_rmt VALUES ('a','e9-tx999', bitShiftLeft(toUInt128(9),64)+999, 0), ('a','e10-tx1', bitShiftLeft(toUInt128(10),64)+1, 0)"
[ "$(ch "SELECT v FROM m0_rmt FINAL")" = "e10-tx1" ] && echo "UInt128 RMT version: PASS" || { echo "RMT FAIL"; exit 1; }
ch "DROP TABLE m0_rmt"
echo "M0: PASS"
