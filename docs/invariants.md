# cdcell invariants

What cdcell can rely on, and what it must design around, when it turns celld's
bucket writes into CDC events. Sources: celld v0.6.3 source
(`/Users/guycohen/dev/celld`, read-only), primary docs (AWS, MinIO, GCS,
Azure, CloudEvents, Debezium, Iceberg, ClickHouse), and the live e2e fleet in
`e2e/` (3 nodes, MinIO, NATS JetStream, toxiproxy), run 2026-11-07.

**Status labels**

| Label | Meaning |
|---|---|
| **GUARANTEED** | A documented contract or a property the code enforces. Rely on it. |
| **BEHAVIOR** | True in v0.6.3 and observed, but not a contract. Use it to optimize; never for correctness. |
| **NOT GUARANTEED** | Can be violated. Design for it. |

**Verified-by:** `code` (read in celld source), `live` (observed in the e2e
fleet or the single-node MinIO setup), `exp` (local experiment), `docs`.

File paths are relative to the celld repo: `R` = `crates/celld/ltx_repl.rs`,
`NL` = `crates/celld/node_log.rs`, `L` = `crates/logic/lib.rs`,
`DB` = `crates/ltx/src/db.rs`, `G` = `docs/guarantees.md`.

---

## A. celld execution and ownership

| ID | Invariant | Scope | Status | Evidence | Verified | cdcell implication |
|---|---|---|---|---|---|---|
| A1 | Exactly one node serves a cell at a time. Ownership is a CAS on `cells/<cell>/own.json` = `{node, epoch}` (conditional create / If-Match). | cell | GUARANTEED for acked writes in the current epoch | `ownership_store.rs:21-31, 951-968`; G:92-97 | code, live | `(cell, epoch, txid)` is a valid stream position. |
| A2 | A node that lost ownership can keep writing, but only into a superseded epoch prefix. | cell | NOT GUARANTEED (stale writes exist) | G:141-145, 269-271 | code, live | Late objects in old epochs are expected; see D. |
| A3 | `own.json` has no expiry. It is valid only while the named node's lease `nodes/<node>.json` is live (`expires_ms > reader's clock`). | cell, fleet | GUARANTEED | `L:4424-4432` | code | Ownership liveness needs the lease, not just `own.json`. |
| A4 | Node lease: TTL `CELLD_TTL_MS` = 10 s, renewed by CAS (If-Match last ETag) every TTL/3 ≈ 3.3 s; fence timer at exactly anchor + TTL on a monotonic clock; on fence the process halts with exit code 3. | node | GUARANTEED | `main/cli.rs:344`; `L:2468-2486`; `actor.rs:4204-4226` | code, live (ETag changed every ~3.3 s) | A lease that stops changing for >10 s means the node is fenced or dead. |
| A5 | No clock-skew margin: peers compare the owner's wall-clock `expires_ms` with their own clock. | fleet | NOT GUARANTEED (skew can cause early takeover) | `L:4427` | code | Never trust timestamps for ordering; bucket-proof acks stay safe, fleet acks rely on sealing. |
| A6 | Every activation, including takeover, advances the epoch (`checked_add(1)`). | cell | GUARANTEED | `L:4097-4112, 1866-1879` | code, live (e1..e10 in an hour) | Epoch boundaries are routine, not rare. |
| A7 | A frozen process (laptop sleep, `docker pause`, GC/VM stall) past the TTL self-fences on wake and restarts with a new generation; its cells move to new epochs. | node | GUARANTEED | `L:3027-3033` | live (`SELF-FENCE … code=3` after Mac maintenance sleep and after `docker pause`) | Expect bursts of epoch changes after any stall. |
| A8 | Inside a cell, one synchronous JS turn runs at a time; awaits interleave. `storage.put` calls queued in one microtask tick flush as one SQLite transaction. | cell | BEHAVIOR | `js/harness.js:1615-1650`; `storage.rs:3571-3594`; `docs/services/durable-objects.md:57-60` | code | Several puts can share one transaction and one TXID. |
| A9 | A generation swap (code deploy) keeps the epoch and continues TXIDs, so app code and schema can change mid-epoch. | cell | BEHAVIOR | `actor.rs:3997-4003` | code | Re-read the schema whenever page 1's schema cookie changes, not only at epoch boundaries. |
| A10 | `own.json` can be released (`{"node":"","epoch":E}`) without changing the epoch; the next claimant writes E+1. | cell | GUARANTEED | `ownership_store.rs:934-949` | code | A released record at epoch E still confirms files of epoch E. |
| A11 | Bucket contract: conditional create and overwrite, read-after-write consistency, ranged reads; list-after-write only for epoch GC. Each node probes this at startup (`probe/` objects). MinIO community passes the probe but is "not qualified for production". | bucket | GUARANTEED (checked at startup) | G:9-40 | docs, code | cdcell can assume strong consistency on any bucket celld runs on. |

## B. LTX format and data path

| ID | Invariant | Scope | Status | Evidence | Verified | cdcell implication |
|---|---|---|---|---|---|---|
| B1 | Per-cell key: `cells/<cellpath>/ltx/e<epoch>/<level:04x>/<min:016x>-<max:016x>.ltx`. TXIDs are 16-digit **hex**; level `0000` = L0, `0001` = L1, `0009` = L9 snapshot. `cellpath` can contain `/facets/…`; an optional fleet key prefix applies. | key | GUARANTEED | `R:2229`; `ltx/src/client/object_store.rs:713-718`; `ltx/src/lib.rs:542-553` | code, live | Parse from the right; parse TXIDs as hex. |
| B2 | Within one epoch's lineage TXIDs go up by exactly 1 per capture pass, with no skips. | cell, epoch | GUARANTEED | `DB:1427-1428`; `ltx/src/replica.rs:276-284` | code, live | Gap detection on contiguous TXID ranges is sound. |
| B3 | A TXID is one capture pass, not one SQLite commit: it never splits a transaction but can merge several. One `db.sync()` can also emit several TXIDs (checkpoint sequence). | cell | GUARANTEED (never splits) / BEHAVIOR (merging) | `ltx/src/wal.rs:542-558`; `DB:1837-1950` | code, live (alice: ~104 writes, 97 TXIDs) | Events are net per TXID; per-statement history is not recoverable. |
| B4 | Merged objects exist: a multi-TXID L0 range (`0004-0007`), L1 folds (every 256 TXIDs or 32 MiB), L9 `1-max` snapshots. Merged files carry the last input's timestamp and WAL fields = 0. | cell | GUARANTEED (they exist) | `R:4141-4160, 4531-4558`; `ltx/src/compactor.rs:146-158` | code, live | Track TXID-range coverage, not file counts. |
| B5 | Full-image L0 files can appear mid-epoch (TRUNCATE-checkpoint boundary snapshot, verify break), recognizable only by `wal_offset = 32` and pages covering `1..commit`. | cell | BEHAVIOR | `DB:1918-1930, 1081-1236`; `R:1062` | code | Treat as a reset point: memcmp against the shadow. |
| B6 | Header flags always include NoChecksum: pre/post-apply checksums are 0. Integrity = file CRC-64 (+ xxh32 inside legacy LZ4 frames). No checksum chain between files. | file | GUARANTEED | `DB:1562`; `ltx/src/ltx.rs:66-103` | code, live (191 files recomputed) | Continuity can only come from TXID ranges. |
| B7 | WAL offset/salt chaining (`off(n) = off(n-1) + size(n-1)`, same salts) holds only between unmerged single-TXID files. | cell | BEHAVIOR | `DB:1572-1586` | live (91/96 links in alice e1) | Optional sanity check; never required. |
| B8 | Two page encodings coexist in one prefix: legacy per-page LZ4 frames (raw L0) and size-prefixed LZ4 blocks (merged/compacted). | file | GUARANTEED (both occur) | `ltx/src/codec.rs:212-280`; `ltx/src/compactor.rs:52` | code, live | Use celld's decoder, which reads both. |
| B9 | The LTX header timestamp is the owner's wall clock (also S3 metadata `Litestream-Timestamp`; `litestream_timestamp` on Azure). Not monotonic across nodes, epochs, or NTP steps. | file | GUARANTEED (source) / NOT GUARANTEED (monotonic) | `DB:975, 1567`; `object_store.rs:76-112` | code, live | Event time only; order by `(epoch, txid)`. |
| B10 | Single-node / bucket mode: each response waits for the LTX PUT plus a GET of `own.json` naming this node and epoch. | cell | GUARANTEED | `logic/output_gate.rs:283-300`; `actor.rs:3906-3941` | code, live | In bucket mode every acked write is in `cells/` before the client sees success. |
| B11 | Bucket-mode per-cell PUTs are usually sequential in TXID order (`syncing` flag), but `sync_wait` bypasses that flag, and retries and L1 compaction run concurrently. | cell | BEHAVIOR (order) / NOT GUARANTEED | `R:4626-4633` vs `R:3164-3166`; `object_store.rs:69-75` | code | Never rely on upload order; buffer by TXID. |
| B12 | Same TXID in many objects: L0, merged L0, overlapping L0 ranges (`a-b` and `a-c`), L1, L9, bundles, bundle repeats, recovery re-uploads, zombie re-uploads. | cell | GUARANTEED (duplicates happen) | `ltx/src/replica_compactor.rs:136-142`; `NL:6705-6745` | code, live | Keep an applied watermark per `(cell, epoch)`; skip `max ≤ hwm`. |
| B13 | Compaction never deletes source files; L1 folds per cell are contiguous. | cell | GUARANTEED | `ltx/src/replica_compactor.rs:1-4, 114-153` | code (re-verified) | **Corrects an earlier claim** that L0 is deleted after compaction. L0 stays until epoch GC. |
| B14 | Large LTX (≥ 5 MiB) uploads as multipart; bundles use a single PUT. | object | GUARANTEED | `object_store.rs:117-120, 888-940`; `bucket.rs:960-967` | code | Subscribe to `ObjectCreated:*` (includes `CompleteMultipartUpload`). |

## C. Fleet mode and bundles

| ID | Invariant | Scope | Status | Evidence | Verified | cdcell implication |
|---|---|---|---|---|---|---|
| C1 | With 2+ nodes (`CELLD_DURABILITY=fleet` default) a write is acked when every ensemble follower (up to 2) has fsynced it: write-all, ack-all, no quorum. | cell | GUARANTEED | `logic/log_tier.rs:136-148`; `NL:278` | code, live (`log ensemble open … members={celld-b, celld-c}`) | Acked rows can sit only on disks for ~2 s. |
| C2 | Each owner uploads one bundle per `CELLD_LOG_FLUSH_MS` (2000 ms) containing only **its own** dirty cells, and only when there are new writes. Followers never upload bundles. | node | GUARANTEED | `R:4697-4740` (`bundle_loop` over the node's cell map) | code, live | Read every bundle from every node; there is no cross-node duplication except across ownership moves. |
| C3 | Bundle key `log/<node>/<generation>/bundle/e<logepoch>-<seq:08>.ltxb`; `<generation>` equals `ownership_index_generation` in `nodes/<node>.json`. The log epoch is the shipper's log epoch, not the cell epoch. | key | GUARANTEED | `NL:6704`; `NL:577-597` (`read_record`) | code, live | The key names its writer; one lease read confirms it (§ rules). |
| C4 | Bundle format: concatenated verbatim L0 files, footer rows `u16 cell_len, cell, u64 cell_epoch, txid, offset, len` (all LE), `u32 LE` footer length, magic `CLB1`. The inner bytes equal the per-cell L0 bytes. | object | GUARANTEED | `ltx/src/bundle.rs:1-12, 58-110` | code, live (decoded) | Same decoder path as per-cell files. |
| C5 | Bundle `seq` comes from a per-session `fetch_add` before the PUT: monotonic per session but with gaps. Flush order is numeric `(epoch, seq)`; `e10-` sorts before `e9-` lexically. | session | GUARANTEED | `NL:6688-6704, 2396-2414` | code | Never infer loss from a missing seq; per-cell TXID continuity is the authority. |
| C6 | A cell's rows ascend within a bundle and are non-decreasing across bundles; an uncredited flush re-sends the same rows. | cell | GUARANTEED | `R:4776-4785`; `NL:5285-5296` | code | Dedupe by `(cell, cell_epoch, txid)`. |
| C7 | In fleet mode the per-cell prefix stays empty by design until an L1 fold (256 TXIDs), eviction (L9), handoff, or recovery. Idle eviction is off by default, so the lag is unbounded, and drained data arrives merged. | cell | GUARANTEED (by design) | `R:3050-3060`; `main/cli.rs:354` | code, live (`cells/` held only `own.json`) | Bundles are the only fresh, per-batch source in fleet mode. |
| C8 | Bundle GC (30 s tick) deletes a bundle once every row is covered per cell; sealed sessions' bundles are deleted after recovery. | session | GUARANTEED | `NL:6285-6385, 6467-6497` | code, live (gone ~30-40 s after coverage) | GET on notification, promptly; treat 404 as "covered elsewhere". |
| C9 | celld credits a bundle only if, after the PUT, the bucket copy of its lease has the same generation and a log that is **Open at the bundle's log epoch**. | session | GUARANTEED | `NL:6673-6750` (`put_bundle`); `logic/log_tier.rs:340-342` | code | **Not** cdcell's rule: this gates ack credit. cdcell confirms with log epoch ≥ the bundle's (C12, § rules). |
| C12 | A healthy session drains every row it ever bundled into per-cell prefixes, even after a log-epoch reconfiguration or an uncredited flush: uncredited rows are re-sent (C6) and draining keeps riding the bundle path while the shipper is degraded. If the session dies, recovery folds every retained bundle of every log epoch (D6). | session | BEHAVIOR | `NL:6753-6764` (`bundle_active` comment), `NL:5262-5280` | code | A bundle whose session is still Open (any log epoch ≥ its own) is history. |
| C13 | A graceful handoff drains a cell into its per-cell prefix **before** ownership moves: eviction publishes an L9 `1-max` snapshot covering every fleet-acked row of the closed database, or falls back to a durable L0 chain (`sync_wait`); release runs a final durability barrier; the `ReleaseOwner` effect is emitted only after the cell stopped. So a bundle row of a session that is still Open, for a cell epoch E lower than the cell's current epoch, is covered by per-cell history the successor restored. | cell | GUARANTEED | `R:3361-3435` (`evict`), `R:3195-3197` (`release`), `L:5119-5135` (`ReleaseOwner` after `StopCause::Evict`) | code | Bundle confirmation (lease Open) need not consult `own.json`. |
| C10 | The Open log is written into the lease **before** the bundle shipper is installed; `bundle_active()` is `inner.is_some()`. A failed write returns before install. | session | GUARANTEED | `NL:5819` (`transition.write(...)?`), `NL:5841` (install), `NL:6753` | code, live (0 of 206 bundle observations had a writer lease without a log) | A lease without a `log` never has bundles of its current generation. |
| C11 | `log: None` is written only by a new generation of the same node, after recovering and sealing its predecessor. Dead-node GC keeps (tombstones) leases that carry a log. | node | GUARANTEED | `NL:4494-4497` (`ensure_predecessors_recovered`); `dead_node_gc.rs:356-405` | code | A bundle whose generation no longer matches the lease is from a sealed session. |

## D. Epochs, phantoms and recovery

| ID | Invariant | Scope | Status | Evidence | Verified | cdcell implication |
|---|---|---|---|---|---|---|
| D1 | Snapshot epoch: opens at TXID 1 with a full image that **also contains the first new write(s)**. Nothing records which TXID of the previous epoch was restored (no cut). | cell | GUARANTEED | `DB:1088-1092, 1552-1553` | code, live (e2/e3 TXID-1 held n=106) | Resolve the boundary by diffing the snapshot against the last confirmed shadow. |
| D2 | Paged epoch (chain ≥ `CELLD_LTX_PAGED_MIN_MB`, default 256): opens with a zero-page marker at `(cut+1)-(cut+1)`, `wal_size = 0`, `commit` = page count at the cut. cut = min − 1 from the key alone. | cell | GUARANTEED | `DB:966-990` (`seed_continuation`); `R:2600-2620` | code (verified) | Exact phantom filtering for paged epochs. |
| D3 | Restore walks epochs from the newest down to one opening at TXID 1, clipping older epochs to `max_txid < next.lo`, and requires the link to end exactly at the cut. | cell | GUARANTEED | `ltx/src/client/epochs.rs:39-80` | code | cdcell's epoch logic must mirror this chain. |
| D4 | Local `.evicted` reuse happens only when `epoch > 1 && !took_over`; the default pressure ownership is `release`, which forces a remote restore. `resume_local` keeps the epoch and continues TXIDs. | cell | GUARANTEED | `logic/restore.rs:27-29`; `machine.rs:105-107`; `L:2105-2121` | code (verified) | Rare paths; the epoch-chain rules still hold. |
| D5 | Takeover order: read `own.json` → owner lease expired → recover the owner's log if Open → CAS `own.json` to E+1 → restore with a **fresh LIST**. No listing is cached across activations. | cell | GUARANTEED | `L:4458-4553, 4655-4690`; `R:2240-2278, 2425` | code | If `own.json` still shows E after a file is visible, every later restore sees the file. |
| D6 | Recovery order: re-check expiry → CAS log Open→Recovering → seal followers → gather follower tails → LIST and fold **every** retained bundle of the dead session (no epoch filter) → mark Sealed. All before the successor's `own.json` CAS. | session | GUARANTEED | `NL:4919-4990` (`recover_serial`), `NL:5262-5330` | code, live (`bundles_read=12 … sealed`) | A bundle that lands after the LIST is never folded. |
| D7 | Recovery runs on demand (takeover) or on a periodic "eager recovery" sweep (observed ~60 s apart). Once started it took < 1 s. | fleet | BEHAVIOR | node logs | live | Suspects usually resolve within seconds to a minute. |
| D8 | Phantoms are always a TXID suffix (`max_txid > cut`), and a fenced owner can leave **several** (unpaced loop, `sync_wait`, compaction, an L9 above the cut). | cell | GUARANTEED (suffix) / NOT GUARANTEED (count) | `R:3164` vs `R:4626`; `epochs.rs:45-47` | code (verified) | Hold the whole unconfirmed tail, not just the last file. |
| D9 | A phantom is not final until a later epoch has an opening object. A sealed log alone decides nothing above recovery's recorded cut. If E+1 writes nothing, E+2 can restore E again and the tail becomes history. | cell | GUARANTEED | G:269-275 | code | Never drop a suspect early. |
| D10 | Legitimate files can land in e\<E\> after `own.json` moved past E (the new owner's tail fold, losing recovery racers, recovery recreating a GC'd prefix). | cell | GUARANTEED (it happens) | `R:2412-2423`; `NL:4809-4837, 6256-6258` | code | "Arrived after `own.json` moved" means unconfirmed, not phantom. |
| D11 | Zombie bundles exist: a PUT already sent by a frozen node lands after recovery sealed its log. In practice their rows are usually duplicates, because recovery gathered them from followers. A phantom row needs a TXID bundled but never shipped to any follower. | session | GUARANTEED (zombies) / BEHAVIOR (usually duplicates) | `NL:6705-6741` | live: pause-only and stall-only produced none; stall + freeze + forced takeover produced bundle seq 12, 7.5 s after the seal, SUSPECT, all 204 rows already history. No phantom rows observed. | Rule must hold suspects; resolution mostly yields duplicates. |
| D14 | An `own.json` notification proves only that a CAS happened at some time, not the record's state after a given GET. | cell | GUARANTEED | G1-G2 (unordered, delayed delivery) | docs | A notification may mark units SUSPECT (epoch > E); it never CONFIRMS. |
| D15 | The first per-cell object of a snapshot epoch in fleet mode is often an L1 `1-256` fold (compaction `base_txid` = 1 for non-continued epochs), with no L0 TXID 1 under `cells/` because TXID 1 went into a bundle. | cell | GUARANTEED | `R:2726` (`base_txid: continuation.map_or(1, …)`) | code, live (e9 held only `L1 tx1-256`) | An opener is any L0 or L1 with `min_txid == 1`, or a paged marker. |
| D12 | Visible ≠ acknowledged. Old-epoch tails can be unacked yet become history; acked rows can appear late (after follower recovery). | cell | GUARANTEED | G:150-236, 269-274 | code | CDC reflects restorable history, decided by the rules below. |

## E. SQLite and page level

| ID | Invariant | Scope | Status | Evidence | Verified | cdcell implication |
|---|---|---|---|---|---|---|
| E1 | One cell = one SQLite DB in WAL mode (`synchronous=NORMAL`, autocheckpoint off; the replicator owns checkpoints). | cell | GUARANTEED | `storage.rs:10-14, 381-382`; `DB:445-454` | code | Shadow state is per cell. |
| E2 | LTX files hold only the **after** image of changed pages. Before-images exist only in cdcell's shadow. | file | GUARANTEED | LTX format | code | Shadow pages are mandatory for any row diff. |
| E3 | A same-size overwrite of an overflowing value can change only overflow pages, leaving the leaf page byte-identical. | page | GUARANTEED (SQLite behavior) | `btreeOverwriteCell` | exp (SQLite 3.41.2: pages [1, 6] changed, leaf page 2 unchanged) | Maintain an overflow-page → owning-leaf map. |
| E4 | User tables from `ctx.storage.sql` live unprefixed in the same DB. Internal tables: `_cf_KV`, `_cf_ALARM`, `_cf_METADATA`, `_cf_FACETS` (WITHOUT ROWID), `_cf_WAKE`, `_litestream_seq`, `_litestream_lock`. | cell | GUARANTEED | `storage.rs:409-430, 477-485`; `DB:357-369` | code | Skip internals; emit user tables and `_cf_KV`. |
| E5 | `_cf_KV(scope,k,v, PRIMARY KEY(scope,k))` is a rowid table plus an autoindex; values are V8 ValueSerializer bytes with version byte 15 (`FF 0F …`); a leading `0x01` marks a stub; TEXT values are legacy JSON. | cell | GUARANTEED | `storage.rs:409-410, 1434-1446`; `js/storage_ops.rs:1009-1055` | code, live (`FF0F49D001` = 104) | Key KV events by `k`, decode V8 to JSON, keep raw bytes. |
| E6 | The authorizer denies ATTACH (and so VACUUM) and most pragmas, so production DBs are effectively auto_vacuum NONE, no VACUUM. | cell | GUARANTEED | `storage.rs:520-562` | code | Fewer page relocations; the decoder still handles them. |
| E7 | Some TXIDs change only internal tables (`_litestream_seq` bump at checkpoints). DB size (`commit`) can shrink. Page size is fixed per DB (read it from each header). Text encoding is fixed at creation (page 1). | cell | GUARANTEED | `DB:1890`; `ltx/src/wal.rs:557-558` | code, live | Empty TXIDs are normal; drop pages beyond `commit`. |

## F. Object storage

| ID | Invariant | Scope | Status | Evidence | Verified | cdcell implication |
|---|---|---|---|---|---|---|
| F1 | Strong read-after-write for PUT, DELETE and LIST on S3 (since Dec 2020), GCS and Azure; MinIO claims the same. | bucket | GUARANTEED | [AWS consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel) | docs | A GET after a notification sees the object or a 404. |
| F2 | celld PUTs are unconditional; keys are immutable in practice, but the same key can be re-PUT (retries, re-taken L9 with a new timestamp, facet delete + recreate within one epoch). | object | NOT GUARANTEED (byte identity) | `object_store.rs:353-357`; `R:3494-3517` | code (facet case inferred) | Dedupe by `(key, ETag)`; a changed ETag on the same key is a reset. |
| F3 | Mutable keys: `own.json`, `nodes/*.json`, deploy pointers, `fleet/*`, `wake/*`, `cells/*/ltx/retired.json`. Immutable in practice: LTX, bundles, deploy versions. | bucket | GUARANTEED | `bucket.rs`, `ownership_store.rs` | code | Never subscribe to `nodes/` (renewed every ~3.3 s). |
| F4 | Deletions celld performs: bundle GC, sealed-session subtrees, epoch GC (only if `CELLD_LTX_RETENTION_SECS > 0`), facet stream deletion, dead-node GC, KV blob sweep, wake entries, probes. | bucket | GUARANTEED (these exist) | `NL:6285-6497`; `R:2026-2048, 3494-3517`; `dead_node_gc.rs`; `kv_blob.rs:95-125` | code, live (bundles) | A 404 is normal; never retry forever. |
| F5 | `LastModified` must not be used as a visibility or ordering signal. | object | NOT GUARANTEED | — | live (judge visibility by GET success time) | Order by `(epoch, txid)`; confirm with reads made after the GET. |
| F6 | Prices (us-east-1 list, verify): PUT/LIST $0.005 per 1k, GET $0.0004 per 1k (LIST ≈ 12.5× GET); SQS standard $0.40 per M; EventBridge $1 per M. | cost | BEHAVIOR | [S3 pricing](https://aws.amazon.com/s3/pricing/), [SQS pricing](https://aws.amazon.com/sqs/pricing/) | docs | GET-on-notify, no LIST in the normal path. |

## G. Notifications

| ID | Invariant | Scope | Status | Evidence | Verified | cdcell implication |
|---|---|---|---|---|---|---|
| G1 | Notifications are not ordered. `sequencer` orders events for the **same key only** (left-pad, then compare). | system | GUARANTEED (no order) | [notification-content-structure](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html) | docs | Reorder per cell by `(epoch, txid)`. |
| G2 | Delivery is at least once: duplicates, typical delay seconds, sometimes a minute or longer. | system | GUARANTEED (at least once) | [EventNotifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html) | docs | Idempotent processing. |
| G3 | Events can be lost: SQS message size limit, misconfiguration, SQS retention (4-14 d), EventBridge's 24 h / 185 retries without a DLQ, MinIO webhook without `queue_dir`. Lifecycle deletes emit no `ObjectRemoved`. | system | NOT GUARANTEED (delivery) | AWS docs; [MinIO notifications](https://github.com/minio/minio/blob/master/docs/bucket/notifications/README.md) | docs | A gap that stays open > ~5 min triggers one targeted LIST. |
| G4 | Payload: `eventName`, `eventTime`, `s3.object.{key, size, eTag, versionId?, sequencer}`; the key is form-URL-encoded (`:` → `%3A`); max 64 KB; an `s3:TestEvent` without `Records` is sent on setup. | event | GUARANTEED | AWS docs | docs | URL-decode keys; ignore test events. |
| G5 | Filters are one prefix + one suffix per rule, with no overlapping rules per event type; middle path segments (the level) cannot be filtered. | system | GUARANTEED | [filtering](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-filtering.html) | docs, live (MinIO accepted the 3 rules) | Rules: `cells/`+`.ltx`, `cells/`+`own.json`, `log/`+`.ltxb`. Drop L1/L9 from the key client-side. |
| G6 | SQS FIFO cannot be a direct S3 destination; EventBridge → FIFO's `MessageGroupId` is static. | system | GUARANTEED | AWS docs | docs | Use SQS standard; order in cdcell. |
| G7 | GCS Pub/Sub and Azure Event Grid: at least once, unordered, duplicates; `objectGeneration` / per-blob `sequencer`. | system | GUARANTEED | GCS, Azure docs | docs | Same design behind a source interface. |
| G8 | e2e: MinIO → NATS JetStream (`queue_dir` set) delivers bucket events with S3-compatible `Records`. | e2e | BEHAVIOR | `e2e/scripts/up.sh` | live (18 messages for claims + bundles) | JetStream mirrors SQS semantics (durable, explicit ack). |

## H. Output-format constraints

| ID | Invariant | Status | Evidence | cdcell implication |
|---|---|---|---|---|
| H1 | CloudEvents 1.0.2: `id`, `source`, `specversion` = `"1.0"`, `type` are required; `source` + `id` unique per event; re-sending an event may reuse its `id`. | GUARANTEED | [spec.md](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md) | Deterministic `id` per row change gives idempotency. |
| H2 | CloudEvents Integer is signed 32-bit. | GUARANTEED | spec.md "Type System" | 64-bit epoch and txid are strings in attributes. |
| H3 | Extension names: lowercase `[a-z0-9]`, ≤ 20 characters recommended. `sequencetype` is removed on `main`. | GUARANTEED | spec.md; `extensions/sequence.md` | Lowercase names only (Debezium's `iodebeziumtxId` breaks this); no `sequencetype`. |
| H4 | Debezium envelope: `before`, `after`, `source`, `op` (c/u/d/r/t/m), `ts_ms`/`ts_us`/`ts_ns`, `transaction`. Debezium 3.7 has an incubating SQLite connector (`source.table`, `source.change_id`, affinity type mapping). | GUARANTEED (format) | Debezium docs (nightly sqlite.html) | Match the SQLite connector's shape. |
| H5 | ClickHouse `ReplacingMergeTree` version must be unsigned or a date type. | GUARANTEED | ClickHouse docs | No packed `__seq` (Iceberg has only signed 64-bit; epochs churn). Order by `(_epoch, _txid)`; for RMT use a UInt128 version computed from the pair (design §14). |
| H6 | Iceberg v4 forbids writing equality deletes; Apache Iceberg's Kafka Connect sink is append-only. | GUARANTEED | iceberg.apache.org spec; issue #17542 | Append-only changelog tables; no delete files. |

## I. e2e environment

| ID | Fact | Verified | Implication |
|---|---|---|---|
| I1 | Anonymous ghcr pulls are denied on this machine. | live | Use local `celld-cdc/celld:v0.6.3`; MinIO and toxiproxy build from source. |
| I2 | celld rejects `--flag=value`; flags and values are separate arguments. | live | Compose command lists use separate items. |
| I3 | A sleeping Mac freezes the nodes past their lease, so they self-fence. | live | Run everything under `caffeinate -i`. |
| I4 | MinIO no longer ships images (`minio/minio`, `quay.io/minio/minio` gone). | live | `e2e/minio/Dockerfile` builds from source. |
| I5 | A process killed with an in-flight PUT through toxiproxy loses the PUT (the connection closes). | live | Zombie tests freeze the sender (stall + `docker pause`) to keep the connection open. |

---

## Summary: rely on it vs design for it

| Rely on it | Design for it |
|---|---|
| One acknowledged writer per `(cell, epoch)`; the epoch is in the key | Stale-owner, fenced-compactor, late-recovery and zombie writes into older epochs, arriving after newer epochs, possibly several files |
| Epochs only increase; every activation advances the epoch | Epoch boundaries are frequent (any stall or restart); prefixes can have gaps or be GC'd |
| TXIDs go up by exactly 1 within a lineage; a TXID never splits a transaction | A TXID can merge transactions; empty and full-image TXIDs; merged and overlapping ranges |
| Snapshot epochs open at TXID 1 (full image + first writes); paged epochs open with a `(cut+1)` marker | Snapshot epochs record no cut |
| Recovery claims the log (CAS) before listing bundles, and folds every bundle it lists | Bundles that land after that LIST (zombies) |
| celld credits a bundle only if its lease shows the same generation and an Open log at the bundle's epoch, read after the PUT | Bundle seq gaps; duplicate rows across bundles; bundles deleted ~30-40 s after coverage |
| Bundling only starts after the Open log is in the lease; `log: None` only after the predecessor is sealed | Per-cell prefixes empty for an unbounded time in fleet mode; drained data arrives merged |
| Compaction never deletes sources; L1 folds are contiguous | 404s from bundle GC, epoch GC, facet deletes |
| Strong read-after-write consistency | Same key re-PUT with different bytes (facet recreate, L9 retry) |
| Notifications only for committed objects; `sequencer` orders one key | Notifications out of order, duplicated, delayed, occasionally lost; keys URL-encoded |
| LTX header carries page size, commit, TXID range, timestamp | Wall-clock timestamps and `LastModified` are not ordering signals; no checksum chain |
| SQLite page semantics (overflow-only rewrites happen) | Schema can change at any TXID, including mid-epoch code deploys |

## cdcell confirmation and phantom rules

The authoritative rules are in [design.md](design.md) §5 (revision 3). In
short, and how they follow from the rows above:

- **Proof rule:** a unit is history once a record that any successor must CAS
  before reading (D5, D6) was read unchanged **after** the unit's GET.
  Bundles: `nodes/<node>.json` with the same generation and `log.state ==
  "open"` at any log epoch ≥ the bundle's (C9 is ack credit, not history;
  C12, C13). Per-cell files: `own.json` epoch == E (D5).
- **Notifications** of `own.json` can only make units SUSPECT (D14).
- **Suspects are parked** in cdcell state and acknowledged (design §5.3).
- **Resolution** (design §5.4): rule 0, per-cell units confirmed by
  `own.json` == E; rule 1, recovery progress records as hints, only for a
  **sealed** writer and a **matching epoch** (open question 15); rule 2, a
  paged marker `(c+1)-(c+1)` cuts the **proven predecessor** epoch (found by
  LIST, mirroring `EpochChain`, D3); rule 3, **any full image of a later
  epoch** (TXID-1 L0/L1, or an L9 `1-max`) is diffed against the shadow. An
  L9 is a valid rule-3 source but is **not** an opener and reveals no cut
  (open question 16). Openers: any L0/L1 with `min_txid == 1`, or a paged
  marker (D15).
- **Phantoms** are final only once a later epoch has an opener (D9).
- **Acknowledge** per design §9 (committed, parked, phantom, 404; duplicates
  only once their covering unit commits).

## Open questions

| # | Question | Status |
|---|---|---|
| 1 | Does a lease without a `log` field ever have bundles? | **Resolved: no.** C10 (Open log written before the shipper installs), C11 (`log: None` only after sealing), live 0/206. |
| 2 | How does the bundle session key map to the lease? | **Resolved.** `<generation>` = `ownership_index_generation`; `read_record` pins it (C3). |
| 3 | Is cdcell's bundle rule safe? | **Resolved (as far as tested).** It is celld's post-PUT record check without the log-epoch equality (C9 vs C12); the zombie test classified the late bundle SUSPECT and every confirmed bundle was history (D11). |
| 4 | Does compaction delete L0 sources? | **Resolved: no** (B13). Corrects the cost report. |
| 5 | Does a preserved restart (`handoff=preserve`) keep the same epoch? | **Resolved: yes,** `resume_local` keeps the epoch and continues TXIDs (D4). |
| 6 | Are writes outside `transaction()` grouped into one SQLite transaction? | **Partly resolved:** puts queued in one microtask tick flush as one transaction (A8); other turn-level grouping unverified. |
| 7 | Are merged L0 and L9 bytes identical across retries and recoverers? | Open. If not, the same key can carry different ETags (F2). |
| 8 | Facet delete + recreate overwriting the same keys within one epoch. | Open (inferred from code). |
| 9 | How often does `sync_wait` race `sync_loop` and produce overlapping or late PUTs? | Open (code reading only). |
| 10 | Phantom rows (bundled but never shipped to any follower): how to reproduce, and does resolution rule 1 drop them correctly? | Open. Zombie test produced only duplicate rows. |
| 11 | Can a fenced compactor in fleet mode fold uncredited local rows into a phantom L1? | Open (`R:2706-2716`). |
| 12 | MinIO event key URL-encoding and loss behavior without `queue_dir`; EventBridge `detail.object.key` encoding. | Open. |
| 13 | Does S3 still coalesce concurrent same-key notifications on unversioned buckets? | Open (old doc text removed, not retracted). |
| 14 | Current cloud prices (quoted from memory). | Open: check in the calculator. |
| 15 | Recovery progress records `log/<session>/recovered/<sha>.json` (`{"version":1,"cells":[{cell,epoch,through}]}`): how reliable? | **Resolved: hints only.** Written once per 32 completed cells with no final flush, and writing stops after the first failure (`node_log/recovery_progress.rs:1-5, 120-145`; `NL:290`). Small recoveries write none. cdcell uses them as a fast path only. |
| 16 | Is an L9 `1-max` evidence of how an epoch opened? | **Resolved: no.** Handoff snapshots are always `TXID(1)..max`, also in paged epochs (`R:3824-3840`). Usable as a diff source, not as an opener. |
| 15 | Clock skew: can a skewed peer take over early and lose a fleet-acked write? | Open (reasoned from code, not tested). |
