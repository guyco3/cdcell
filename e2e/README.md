# cdcell end-to-end stack

A production-shaped celld fleet on one machine, to develop and test cdcell
against real bucket traffic.

```
client ─► lb :8180 ─► celld-a/b/c (:8181-8183 direct)   peer HTTP + replication between nodes
                         │ each node's S3 traffic goes through toxiproxy (fault injection)
                         ▼
                      minio :9100 (bucket `celld`, console :9101)
                         │ bucket notifications (cells/*.ltx, cells/*own.json, log/*.ltxb)
                         ▼
                      nats :4222 JetStream stream CELLD (subject celld.objects)
```

Phase 2 (customer side, `scripts/cdc-up.sh`): Lakekeeper v0.15.0 on
Postgres 17 (REST catalog :8281, UI at /ui), `minio-cdc` :9200 holding
warehouse `cdc` in bucket `warehouse`, ClickHouse 26.9 (:8123,
default/clickhouse). M0 verified: Lakekeeper multi-table commits are atomic;
ClickHouse reads through `DataLakeCatalog` with vended credentials; the
UInt128 ReplacingMergeTree version works.

## Run

```sh
caffeinate -i ./scripts/up.sh          # storage, broker, notifications, deploy, fleet
./scripts/load.sh 300 8 10             # 300 random mutations over 10 shops
python3 -I tools/inspect.py leases     # node leases + node-log state
python3 -I tools/inspect.py bundles -n 6
python3 -I tools/inspect.py cells      # owner, epoch, LTX objects per epoch/level
caffeinate -i ./scripts/zombie-test.sh zombie   # late bundle after recovery
caffeinate -i ./scripts/cdc-up.sh      # customer side: CDC MinIO, Lakekeeper, ClickHouse
./scripts/m0-verify.sh                 # M0 gate: Iceberg via Lakekeeper, read by ClickHouse
docker compose down -v                 # tear down, including data
```

Needs `docker`, `mc`, and `celld` + `esbuild` on the host (deploys run from
the host). Images: `CELLD_IMAGE` (default `celld-cdc/celld:v0.6.3`, built
from the celld repo's Dockerfile); MinIO and toxiproxy build from source.

**Keep the Mac awake** (`caffeinate -i`). A sleeping laptop freezes the nodes
past their 10 s lease; they self-fence (exit 3) and restart, moving every
cell to a new epoch.

## Demo Worker

`worker/shop` is one Durable Object per shop with SQL tables chosen to
exercise the decoder: `orders` (rowid), `items` (WITHOUT ROWID, composite
PK), `attachments` (5-20 KB blobs that spill into overflow pages, plus
same-size overwrites that change only overflow bytes), and the KV API.
`?shop=NAME&op=dump` returns the object's real state for verification.

## Zombie test

`scripts/zombie-test.sh MODE` makes the busiest node lose its lease under
load and checks cdcell's bundle rule against what the fleet kept.

| mode | fault | what it shows |
|---|---|---|
| `pause` | `docker pause` 25 s | node self-fences on wake, before its next bundle tick: no late bundle |
| `stall` | upload latency 20 s | node fences and exits; its in-flight PUT dies with the connection |
| `zombie` | latency 25 s, freeze the node, force takeovers | an in-flight bundle lands after recovery sealed the log |

The rule: after GETting a bundle `log/<node>/<generation>/bundle/e<E>-<seq>.ltxb`,
read `nodes/<node>.json`. If it carries the same generation and a `log` that
is `open` (at any log epoch ≥ E), recovery has not listed the bundles yet and
must see this one: CONFIRMED. It is celld's own post-PUT check
(`node_log.rs` `put_bundle` → `read_record`) minus the log-epoch equality,
which celld needs for ack credit but which would wrongly hold healthy
bundles after an ensemble reconfiguration bumps the log epoch (recovery
folds bundles of every log epoch).

A lease with no `log` never has bundles of its current generation: celld
writes the Open log into the lease before it installs the bundle shipper
(`node_log.rs` `maintain`, `transition.write(...)?` then `inner` install),
and only a new generation writes `log: None`, after recovering and sealing
its predecessor (`ensure_predecessors_recovered`). A bundle whose generation
no longer matches the lease is therefore from a sealed session: SUSPECT.
Resolution follows docs/design.md §5.4: the next epoch with an opener decides
(paged marker = exact cut; TXID-1 snapshot = diff). Recovery's progress
records are only hints. Raw per-cell coverage is never used to decide,
because a zombie can upload per-cell files after the seal; zombie_report.py
uses coverage only as an approximate ground truth for the report.

First `zombie` run (2026-11-07): 12 bundles CONFIRMED and all in history;
bundle seq 12 landed 7.5 s after recovery sealed (`bundles_read=12`),
classified SUSPECT, and all its rows (tx 267-292 on 8 cells) were already
in history because recovery had gathered them from the followers' disks.
