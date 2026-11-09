#!/usr/bin/env bash
# Random mutations against the fleet through the load balancer.
#   load.sh [REQUESTS=300] [PARALLEL=8] [SHOPS=10]
set -euo pipefail
N=${1:-300}; P=${2:-8}; SHOPS=${3:-10}
URL=${URL:-http://127.0.0.1:8180}
seq 1 "$N" | awk -v s="$SHOPS" '{ printf "shop-%02d\n", ($1 % s) + 1 }' \
  | xargs -P "$P" -I{} curl -s -m 30 -o /dev/null -w "%{http_code}\n" "$URL/?shop={}&op=random" \
  | sort | uniq -c | sed 's/^/  HTTP /'
