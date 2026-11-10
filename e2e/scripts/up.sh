#!/usr/bin/env bash
# Bring up the e2e fleet: MinIO + NATS JetStream, bucket notifications,
# the demo Worker deployed, then three celld nodes behind a load balancer.
#
# Needs on the host: docker, mc, celld (+ esbuild) for `celld deploy`.
# Run under `caffeinate -i` on a laptop: a sleeping Mac freezes the nodes past
# their 10 s lease and they self-fence (exit 3, then restart).
set -euo pipefail
cd "$(dirname "$0")/.."

S3_HOST=http://127.0.0.1:9100
NET=cdcell-e2e_fleet
for bin in docker mc celld esbuild; do
  command -v "$bin" >/dev/null || { echo "missing $bin on PATH" >&2; exit 1; }
done

echo "== storage and broker"
docker compose up -d --wait minio nats
mc alias set e2e "$S3_HOST" minioadmin minioadmin >/dev/null
mc mb --ignore-existing e2e/celld >/dev/null

echo "== JetStream stream CELLD (subject celld.objects, 14 day retention)"
docker run --rm --network "$NET" natsio/nats-box:latest \
  nats -s nats://nats:4222 stream add CELLD --subjects celld.objects \
  --storage file --retention limits --max-age 14d --discard old --replicas 1 \
  --dupe-window 2m --defaults >/dev/null 2>&1 || echo "   (stream exists)"

echo "== MinIO -> NATS notifications"
# queue_dir persists events while NATS is unreachable instead of dropping them.
docker compose exec -T minio mkdir -p /data/.minio-events
mc admin config set e2e notify_nats:cdcell \
  address=nats:4222 subject=celld.objects jetstream=on \
  queue_dir=/data/.minio-events queue_limit=1000000 >/dev/null
mc admin service restart e2e --json >/dev/null 2>&1 || true
until curl -sf "$S3_HOST/minio/health/live" >/dev/null; do sleep 1; done
ARN=arn:minio:sqs::cdcell:nats
mc event rm e2e/celld "$ARN" --force >/dev/null 2>&1 || true
mc event add e2e/celld "$ARN" --event put --prefix cells/ --suffix .ltx
mc event add e2e/celld "$ARN" --event put --prefix cells/ --suffix own.json
mc event add e2e/celld "$ARN" --event put --prefix log/ --suffix .ltxb
mc event ls e2e/celld

echo "== deploy the shop Worker"
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  celld deploy worker/shop --bucket s3://celld --endpoint "$S3_HOST" --region us-east-1 2>&1 \
  | grep -vE "jemalloc|allocator will not"

echo "== fleet"
docker compose up -d celld-a celld-b celld-c lb
for _ in $(seq 1 60); do
  curl -sf -m 5 "http://127.0.0.1:8180/?shop=smoke&op=create" >/dev/null && break
  sleep 1
done
curl -s -m 5 "http://127.0.0.1:8180/?shop=smoke&op=create"; echo
echo "fleet is up: http://127.0.0.1:8180  console: http://127.0.0.1:9101 (minioadmin/minioadmin)"
