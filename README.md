# cdcell

Change data capture for [celld](https://github.com/denoland/celld) Durable
Objects. cdcell reads what celld already writes to its bucket (LTX page
images and fleet bundles), decodes them into row-level changes, and writes an
**append-only change log** per Durable Object class and SQLite table as
Apache Iceberg tables in your object storage. Your own engine (ClickHouse,
Spark, Trino, …) builds current state (SCD type 1) and history (type 2) on
top: see **[docs/consumer-guide.md](docs/consumer-guide.md)**.

Each row: `__cell_id`, `__epoch`, `__txid` (truthful order), `__op`
(`c`/`u`/`d`/`r`), `__key`, `__ts`, `__schema_version`, `__ingested_at`, then
the table's own columns.

It installs nothing inside celld or your Durable Objects, and it never emits
a write that celld did not keep (phantoms from fenced nodes are resolved
before emission).

```
celld bucket ──notifications──► broker (SQS | NATS JetStream)
   ▲  GET objects, own.json, nodes/*.json, rare LIST          │
   └──────────────────────────────── cdcell (one process in v1)
        intake → per-cell confirm/apply/decode → committer ──► Iceberg REST catalog ──► your bucket
```

## Status

v1 in progress on branch `v1-foundation`. Milestones:

| | Milestone | Gate |
|---|---|---|
| M0 | Customer-side stack (Lakekeeper, ClickHouse) | ✅ `e2e/scripts/m0-verify.sh` |
| M1 | Page → row decoder (`cdcell-core`) | ✅ `cargo test -p cdcell-core` |
| M2 | Streaming service, single node | ✅ `e2e/scripts/m2-gate.sh` |
| M3 | Fleet mode (bundles, lease confirmation) | ✅ `e2e/scripts/m3-gate.sh` |
| M4 | Epochs and phantoms under faults | ✅ `e2e/scripts/m4-gate.sh` |
| M5 | Crash safety, lost notifications, outages, bootstrap, resync after L1 fold | ✅ `e2e/scripts/m5-gate.sh`, `e2e/tests/m5-{bootstrap,no-shadows,l1-resync}.sh` |
| R5 | Plain change log, schema registry and evolution | ✅ `e2e/tests/r5-schema-evolution.sh` |
| M6 | Operations: metrics, maintenance, IAM, soak | ⏳ metrics/maintenance/IAM ✅, 60-minute soak not yet run |

Details, deviations from the design, and open items:
[`docs/design.md` §9](docs/design.md).

## Layout

| Path | What |
|---|---|
| `docs/consumer-guide.md` | **For customers:** the change-log contract and recipes (ClickHouse, Trino, Spark; merge-on-read, copy-on-write, type 2) |
| `docs/design.md` | The v1 design, revision 5 (contract, worker model, confirmation, schemas, commits) |
| `docs/invariants.md` | What celld, object storage and notifications guarantee, with code evidence |
| `crates/cdcell-core` | No-I/O library: LTX/bundle decoding, SQLite page → row diffs, V8 KV values |
| `crates/cdcell` | The service: intake, per-cell pipeline, Iceberg output, commits, recovery, metrics |
| `deploy/iam/celld-read-policy.json` | Least-privilege read policy for the celld bucket |
| `e2e/` | Local 3-node celld fleet + MinIO + NATS + Lakekeeper + ClickHouse, gates and tests |

## Build

The Rust workspace depends on celld's `celld-ltx` crate by path
(`../celld/crates/ltx`), so check out celld next to this repo. Builds run in a
container (`scripts/cargo.sh`, image `celld-cdc/rust-dev:1.98.1`, built from
the celld repo's Dockerfile `build` stage):

```sh
scripts/cargo.sh build -p cdcell
scripts/cargo.sh test -p cdcell-core     # decoder property test + real celld fixture
```

## Run

```sh
cdcell run          # notifications → Iceberg (configured by CDCELL_* env vars, see crates/cdcell/src/run.rs)
cdcell maintain     # expire snapshots, set metadata-cleanup properties on cdcell's tables
```

Prometheus metrics on `:9464` (`cdcell_watermark_age_seconds_p99`,
`cdcell_parked_cells`, `cdcell_s3_requests_total{op}`, …; design §8).

## Development

Red/green TDD: every change starts with a failing test (Rust test or an
`e2e/tests/` / `e2e/scripts/` scenario), seen red, then made green. Run every
gate with `e2e/scripts/all-gates.sh`; see [`e2e/README.md`](e2e/README.md).
