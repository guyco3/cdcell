#!/usr/bin/env bash
# Bring up the customer's side: CDC MinIO, Lakekeeper (+ Postgres), ClickHouse,
# and create the Iceberg warehouse `cdc` in bucket `warehouse`.
# Idempotent: safe to rerun.
set -euo pipefail
cd "$(dirname "$0")/.."
LK=http://127.0.0.1:8281

echo "== services"
docker compose up -d --wait minio-cdc lakekeeper-db lakekeeper clickhouse

echo "== bucket"
mc alias set cdc http://127.0.0.1:9200 minioadmin minioadmin >/dev/null
mc mb --ignore-existing cdc/warehouse >/dev/null

echo "== lakekeeper bootstrap"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$LK/management/v1/bootstrap" \
  -H 'Content-Type: application/json' -d '{"accept-terms-of-use": true}')
case "$code" in 2*) echo "   bootstrapped";; *) echo "   bootstrap HTTP $code (already bootstrapped is fine)";; esac

echo "== warehouse cdc"
if curl -sf "$LK/management/v1/warehouse" | grep -q '"name":"cdc"'; then
  echo "   exists"
else
  curl -sf -X POST "$LK/management/v1/warehouse" -H 'Content-Type: application/json' -d '{
    "warehouse-name": "cdc",
    "project-id": "00000000-0000-0000-0000-000000000000",
    "storage-profile": {
      "type": "s3", "bucket": "warehouse", "key-prefix": "cdc",
      "endpoint": "http://minio-cdc:9000", "sts-endpoint": "http://minio-cdc:9000",
      "region": "us-east-1", "path-style-access": true,
      "flavor": "s3-compat", "sts-enabled": true
    },
    "storage-credential": {
      "type": "s3", "credential-type": "access-key",
      "access-key-id": "minioadmin", "secret-access-key": "minioadmin"
    }
  }' && echo
fi
curl -s "$LK/catalog/v1/config?warehouse=cdc"; echo
echo "Lakekeeper UI: $LK/ui   ClickHouse: http://127.0.0.1:8123 (default/clickhouse)"
