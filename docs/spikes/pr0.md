# PR 0 spikes (celld v0.6.3 @ 8702f6d, iceberg 0.10.1, Lakekeeper 0.15.0)

Sources: celld `/Users/guycohen/dev/celld` (read-only), iceberg crate in docker volume
`cdcell-cargo-registry` (`/r/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.10.1`), the live e2e
Lakekeeper. Nothing was built and no cdcell file was changed. Probe scripts and output are in
`scratchpad/probe/s7.py` to `s10.py` and `out7.txt` to `out9.txt`.

---

## 1. L0 straddles (design M8, §2.1)

**Verdict.** Yes. A per-cell L0 can start at or below the max of existing L1s in the same epoch:
an L0 `1-600` over L1s `1-256`, `257-512` is a real shape. celld's own compactor says so
(`crates/ltx/src/replica_compactor.rs:136-141`: "A direct drain can start before `seek` and still
contain the only copy of its uncovered tail"). Within an epoch, an L0 straddles **L1s (and in one
fallback case an L9)**. It never starts below the end of an earlier contiguous per-cell L0, except
in the reload-with-hole edge case below. So a plain "L0 StartAfter = boundary" LIST is **not**
sufficient when the cursor ended on an L1. The full-L0 fallback is required. A cheaper first LIST is
safe, see the rule below.

### Why
- The drain start is the replica's *uploaded position*: `from = replica.pos().txid + 1`
  (`ltx_repl.rs:4110`). It uploads `from..=dpos` as one merged L0 (`:4152-4160`, merge needs at
  least 2 contiguous rows, `:2165-2172`), or n-n per row if the merge fails.
- `replica.pos` moves only in two places: at activation (`seed_pos(seed)`, `:2656`) and after a
  successful `sync_cell` upload (`seed_pos(last)`, `:4199`). celld never calls `Replica::sync()`, so
  the `pos = ZERO` reset in `replica.rs:240` is not reachable.
- The bundle loop credits `durable_txid` but "the replica's own position deliberately does NOT
  advance" (`ltx_repl.rs:4689-4696`). Compaction does not touch `pos` either. It stores only
  `compacted_txid` (`:4377-4379`). With the bundle overlay (`:2715-2722`), L1s are folded from bundle
  rows that were never per-cell L0s, so L1 coverage runs ahead of `pos`. The next drain therefore
  starts at `pos+1`, which can be far below the L1 max.

### Every per-cell upload path and its range start rule
| Path | Level / range | Start rule | Citation |
|---|---|---|---|
| `sync_cell` (drain): eviction fallback (`evict`, when the L9 is skipped or fails), `release` (StopMode::Rebase = cleanup), unpaced/degraded `sync_loop`, every `sync_wait` | L0 `a-b`, merged (or n-n per row) | `a = replica.pos + 1`. pos = activation seed, or the end of the last successful `sync_cell` in this handle. **Can be ≤ an L1 max** (straddle). `b = db.pos` ≥ every L1 max (L1s only fold captured TXIDs) | `ltx_repl.rs:4100-4200`, `3420-3445`, `3468-3480`, `4596-4620`, `3156-3170` |
| bucket mode (no shipper) per write | L0 `n-n` normally. **Merged `a-b` after an upload failure**: the next capture adds a TXID, and the retry uploads both rows merged | same as above | `:4596-4620` (`paced=false`), `:4152` |
| Recovery `upload_gathered` (dead-leader recovery, and `fold_cell` of a quiet predecessor epoch at activation, `:2412-2421`) via `upload_raw_l0` | L0 merged, or n-n | the first gathered row `> watermark`. watermark = `covered_txid` = max(percell_txid, compacted L1 max, remembered), or for a non-resident cell **one LIST max over all levels**. So it starts above every L0/L1/L9 max. No straddle. It can start above `watermark+1` if rows are missing | `node_log.rs:4762-4820`, `ltx_repl.rs:1822-1872`, `client/object_store.rs:735-747`, `ltx_repl.rs:2139-2155` |
| Paged epoch marker | L0 `m-m`, `m = chain.max_txid()+1` | the first object of the epoch. seed = m, so the first drain starts at m+1 | `ltx_repl.rs:2495`, `2604-2620`, `db.rs:961-985` |
| Compaction L1 (`ReplicaCompactor`, overlay) | L1 `seek-max` | `seek = max(prev L1 max + 1, base)`. base = 1, or m for a paged epoch (`:2726`). L1s are contiguous and never overlap. The output is clipped at `seek` even when a source L0 straddles (`compact_from(min_txid,…)`). **Sources are never deleted** ("leaves every source object intact", `replica_compactor.rs:100-102`). The cdcell kit's `fold_l1` deletes L0s and always folds from 1, which celld does not do | `replica_compactor.rs:109-200`, `393-404`, `ltx_repl.rs:4317-4380` |
| Handoff L9 (`evict`, a non-paged cell within the snapshot budget) | L9 `1-max` | always 1 | `ltx_repl.rs:3819-3850`, `3361-3410` |

Notes:
- **Idle eviction (`CELLD_IDLE_EVICT_S`) normally writes an L9, not a drain L0.** `evict_idle` →
  `begin_eviction` → `StopMode::Evict` → `evict` publishes the L9 first. It drains an L0 only
  when the cell is paged (`prepare_handoff_snapshot` returns None, `:3743-3750`), when the snapshot
  exceeds the budget (`durability_timeout_ms * SNAPSHOT_BYTES_PER_MS`, `:1630`, `:3771-3796`), or when the
  publish or visibility check fails. A published-but-not-visible L9 followed by a drain gives an L0
  `pos+1..max` that straddles that L9 (`:3405-3445`). Cleanup (`StopMode::Rebase`) always drains
  (`runtime.rs:1494`, `ltx_repl.rs:3195-3203`). The design text "an eviction can write L0 `1-600`"
  holds only for these cases.
- **Paged straddle shape:** L1 `m..m+255` (it includes the marker), then a drain L0 `m+1..N`.
- **Reload edge:** a same-epoch reload with a local db (`dst_existed && !restored`) seeds
  `min(local pos, contiguous_covered_txid(L0+L1 from 1))` (`:2569-2573`, `2621-2628`, `2114-2134`).
  If the epoch has a hole, or is a paged epoch whose files start at m > 1 (covered = 0), the
  next drain can start below an existing L0's end. This is rare, but it is the one case where an L0 starts
  below an earlier L0 end. The full-L0 fallback covers it.
- An L0 lying entirely inside an L1 is possible (per-row fallback after a straddle, or a repeated
  recovery upload, which is idempotent by key). It is harmless.

### Implication: LIST policy for a gap inside epoch E, cursor `t`
- L1 and L9: list in full (unchanged).
- L0, first LIST: `StartAfter = key(E, 0, s, ffffffffffffffff)`, where **`s` = the highest max among
  L0 keys of epoch E that cdcell knows, with max ≤ t**. Use the marker TXID for a paged epoch if
  nothing higher is known. With no L0 known, list `0000/` from the start. This is safe with incomplete
  knowledge: celld's next drain starts at `true_s + 1`, and `true_s` ≥ every `sync_cell` L0 end.
  A known end `s ≤ true_s` therefore yields a superset. When the cursor's last step was an L0 ending at
  t, then `s = t`, which is the design's current rule. That makes "StartAfter = boundary" safe
  **exactly when the boundary is the end of a per-cell L0 (or the marker)**, and unsafe when it is
  an L1 or L9 end.
- L0, second LIST (gap still open): `0000/` in full, from TXID 0. This closes the reload-hole edge.
- Keep `gap_not_closed_by_start_after_lists_l0_in_full`, and add a planner test:
  `cursor_on_l1_start_after_uses_last_known_l0_end`.

### Test-kit primitives needed (tests/common/mod.rs)
- Per-cell `replica_pos: u64` in `CellSim`. `activate` sets it to 0. `activate_paged` sets it to the
  marker TXID. Unpaced single-node `exec` advances it per write. Fleet `exec`, `flush` and
  `fold_l1*` do **not** advance it.
- `drain_l0_from(cell, from: Option<u64>)`, celld's `sync_cell`: one merged L0
  `from.unwrap_or(replica_pos+1) ..= c.txid`, the union of pages with commit = the last row's commit
  (as `drain_merged` builds it), or n-n when only one row. Then `replica_pos = c.txid`. The explicit
  `from` lets a test generate the reload-hole edge. The kit must keep every capture's pages per
  TXID (bundles may have been GC'd).
- `fold_l1_next(cell)`, celld's compactor: L1 `max(prev_L1_max+1, base) ..= c.txid`, which **keeps L0s**.
  Keep the old `fold_l1` only as a legacy shape, or remove it.
- `upload_recovered(cell)`: L0 from `max(all-level max)+1` (never straddles), to cover recovery.
- `evict_l9_then_drain(cell)`: the L9 `1-max`, then a drain from `replica_pos+1` (the not-visible-L9
  fallback).
- The PR 0 assertion: in fleet mode, exec 600, `fold_l1_next` at 256 and 512, `drain_l0_from(None)`
  gives keys `0001/…1-…100`, `0001/…101-…200`, `0000/…1-…258` (hex). Generators interleave
  {exec, flush, fold_l1_next, drain_l0_from, evict_l9_then_drain, upload_recovered, activate,
  activate_paged} and drop notifications at random. Property: StartAfter(s_known), then the full-L0
  fallback, closes every gap the celld restore plan can close.

---

## 2. Paged marker identification (B1)

**Verdict.** The predicate "one TXID, zero pages, `wal_size == 0`" identifies real markers, and no
real per-cell or bundle capture matches it. `cdcell-core/src/ltx.rs:25-30` `is_paged_marker`
already implements it correctly (`min==max && pages.is_empty() && wal_size==0 && min>1`).

### Marker as written (`crates/ltx/src/db.rs:961-985`, uploaded at `ltx_repl.rs:2614-2620`)
`version=VERSION, flags=HEADER_FLAG_NO_CHECKSUM, page_size, commit=<db page count at the cut>
(map.commit, :2495), min_txid=max_txid=m (= prior chain max + 1, so ≥ 2), timestamp=now,
pre_apply_checksum=0, wal_offset=32 (WAL_HEADER_SIZE), wal_size=0, wal_salt1/2=current WAL header
salts, node_id=0`. Encoded with `encode_file(&header, &[], 0)`: **zero pages**. Key:
`e<E>/0000/<m>-<m>.ltx`. It is the epoch's first object, and the epoch's compaction base is m (`:2726`).

### A normal single-TXID L0 (`db.rs:1428-1577`)
`min=max=pos+1`, `wal_offset=info.offset`, `wal_size = sz = max_offset - info.offset`. A capture
with `sz == 0` returns early unless it is a snapshot (`db.rs:1541-1545`: "Exit if there are no new
WAL pages and we are not snapshotting"). A snapshotting capture has pages `1..=commit` without the
lock page (`collect_snapshot_pages`, `:1693-1709`), so it is non-empty for any real db. A no-op
commit (`BEGIN; COMMIT;`) writes no WAL frames, so no capture and no TXID. A non-snapshot capture
always has `wal_size > 0`, even if its page list could be filtered. So **no capture is
zero pages with `wal_size==0`**. Merged L0s and L1s have `min < max`. L9s (`db.rs:2095-2141`) have
`min=1`, `pages=1..commit`.

### Implication
- Use `Unit::is_paged_marker` as is. A planner proof of cut `c` = an object of `E'+1` with
  `is_paged_marker()` and `min = c+1`. `commit` can also be checked against the state's page count at
  c (a cheap sanity assert).
- Test-kit caveat: the kit's `encode` sets `wal_size: 0` for *every* L0 (`tests/common/mod.rs`
  `encode`). Its normal L0s still fail the predicate only because they have pages. Give kit captures
  `wal_size > 0` (for example `pages*(page_size+24)`) so `zero_page_marker_header_proves_cut` and the
  negative test (`n-n` with pages is not a marker) exercise both conditions.
- Design wording: "in bucket mode every L0 is `n-n`" is nearly true. An upload failure makes the
  retry a merged `a-b` (see the table in §1). It does not affect marker identification.

---

## 3. Lakekeeper 0.15.0 partition-spec and sort-order evolution

**Verdict.** It works with one atomic commit, `add-spec` + `set-default-spec(-1)` +
`add-sort-order` + `set-default-sort-order(-1)` under `assert-default-spec-id` and
`assert-default-sort-order-id`. `replace-sort-order` **does not exist** in the REST API (422).

The probe used scratch namespace `spec_probe_38661e` (created, then tables dropped with
`purgeRequested=true` (204), then the namespace dropped (204), then a GET returned 404). Prefix
`01a1c4bd-170e-701d-aeef-bc3ed502d600`.

Create (`POST /namespaces/{ns}/tables`) → **200**:
```json
{"name":"t","schema":{"type":"struct","schema-id":0,"fields":[
 {"id":1,"name":"__cell_id","required":true,"type":"string"},{"id":2,"name":"__key","required":true,"type":"string"},
 {"id":3,"name":"__epoch","required":true,"type":"long"},{"id":4,"name":"__txid","required":true,"type":"long"},
 {"id":5,"name":"__op","required":true,"type":"string"},{"id":6,"name":"__ts","required":true,"type":"timestamptz"},
 {"id":7,"name":"v","required":false,"type":"string"}]},
 "partition-spec":{"spec-id":0,"fields":[
  {"source-id":6,"field-id":1000,"name":"__ts_day","transform":"day"},
  {"source-id":1,"field-id":1001,"name":"__cell_id_bucket","transform":"bucket[16]"}]},
 "properties":{"format-version":"2"}}
```
The returned metadata has `default-spec-id 0`, `last-partition-id 1001`, and `default-sort-order-id 0` (unsorted).

Commit (`POST /namespaces/{ns}/tables/t`) → **200**:
```json
{"requirements":[{"type":"assert-default-spec-id","default-spec-id":0},
                 {"type":"assert-default-sort-order-id","default-sort-order-id":0}],
 "updates":[{"action":"add-spec","spec":{"fields":[{"source-id":6,"field-id":1000,"name":"__ts_day","transform":"day"}]}},
            {"action":"set-default-spec","spec-id":-1},
            {"action":"add-sort-order","sort-order":{"order-id":1,"fields":[
              {"source-id":1,"transform":"identity","direction":"asc","null-order":"nulls-first"},
              {"source-id":2,"transform":"identity","direction":"asc","null-order":"nulls-first"},
              {"source-id":3,"transform":"identity","direction":"asc","null-order":"nulls-first"},
              {"source-id":4,"transform":"identity","direction":"asc","null-order":"nulls-first"},
              {"source-id":5,"transform":"identity","direction":"asc","null-order":"nulls-first"}]}},
            {"action":"set-default-sort-order","sort-order-id":-1}]}
```
Result: `default-spec-id 1`, with spec 1 = `[{"source-id":6,"field-id":1000,"name":"__ts_day","transform":"day"}]`.
Spec 0 is retained, `last-partition-id` stays 1001, and `default-sort-order-id` is 1 with the 5 fields.

Other results:
- The same commit again (stale) → **409** `{"type":"CatalogCommitConflicts","message":"CatalogCommitConflicts, context: { expected: 0, found: 1 } => Requirement failed: Default partition spec id does not match"}`.
- Re-adding an identical spec (with `assert-default-spec-id 1`) → **200**, a no-op. No spec 2 is created; Lakekeeper dedups to the existing spec 1.
  This holds even when the body carries `"spec-id":0`, which is ignored.
- `add-spec` **without `field-id`** → 200, and Lakekeeper reuses `field-id 1000` for the identical
  `day(__ts)` field. The field-id can be omitted.
- `replace-sort-order` → **422** `unknown variant replace-sort-order, expected one of upgrade-format-version, assign-uuid, add-schema, set-current-schema, add-spec, set-default-spec, add-sort-order, set-default-sort-order, add-snapshot, set-snapshot-ref, ...`.
- **Gotcha:** on create, Lakekeeper **renumbers schema field ids** (fresh ids in order) and rewrites the
  spec's `source-id`s to match. A table created with ids {1, 6} came back as {1, 2}. A later `add-spec`
  naming `source-id 6` then gets **400** `InvalidTableUpdate "Cannot find partition source field with id 6 in schema"`.

### Implication
- `it_rev5_table_add_spec_and_sort_order`: use `add-sort-order` (not `replace-sort-order`), with
  `set-default-*` set to -1, under `assert-default-spec-id` and `assert-default-sort-order-id`, all in one commit. A 409
  `CatalogCommitConflicts` means another plan evolved the table first: reload and re-check, since the evolution is idempotent.
- Always take `source-id`s from the **loaded** table metadata (look up `__ts` and `__cell_id` by name). Never
  take them from the ids cdcell sent at create.
- iceberg-rust's `Transaction` has `replace_sort_order()` (`transaction/sort_order.rs`), which emits
  `add-sort-order` + `set-default-sort-order`. Only the REST action name is missing. The spec change must be sent as raw
  REST (`TableUpdate::AddSpec` / `SetDefaultSpec` via the catalog `update_table`), or through the cdcell
  raw-commit path used for the fence.

---

## 4. iceberg 0.10.1 DataFile JSON across processes

**Signatures** (`src/spec/manifest/mod.rs:125-156`):
```rust
pub fn serialize_data_file_to_json(data_file: DataFile, partition_type: &StructType,
                                   format_version: FormatVersion) -> Result<String>
pub fn deserialize_data_file_from_json(json: &str, partition_spec_id: i32,
                                       partition_type: &StructType, schema: &Schema) -> Result<DataFile>
```

**What survives** (`src/spec/manifest/_serde.rs:99-240`): content, file_path, file_format, partition
(a `RawLiteral` typed by `partition_type`), record_count, file_size, column_sizes, value_counts,
null/nan counts, lower/upper bounds (bytes), key_metadata, split_offsets, equality_ids,
sort_order_id, first_row_id, referenced_data_file, content_offset/size.
- **`partition_spec_id` is not in the JSON.** The caller passes it, so cdcell must store it next to the
  JSON (the plan records spec ids, which fits).
- **Bounds are decoded through the given `schema`.** A bound whose field id is not in that schema is
  **silently dropped** (`parse_bytes_entry`, `_serde.rs:248-275`). So pass a schema that contains every
  column the file has. Use the staged schema id (`metadata.schema_by_id`) or the current schema, which is safe because cdcell only adds columns
  and never promotes types.
- The partition is decoded with `partition_type`, which must be the type of the spec the file was written
  for (`spec.partition_type(&schema)`).
- **The JSON is not byte-deterministic:** count and bound maps come from a `HashMap` and are
  sorted only under `cfg(test)` (`_serde.rs:300-316`). Do not hash or compare the JSON text. Compare
  it parsed, or canonicalize it (sort the entries by key) before hashing.

**fast_append with deserialized files** (`src/transaction/append.rs:58-120`,
`snapshot.rs:141-163`, `276-300`): `add_data_files` accepts any `DataFile`, and validation requires
(a) content = Data; (b) **`data_file.partition_spec_id == table.default_partition_spec_id()`**,
otherwise `DataInvalid "Data file partition spec id does not match table default partition spec id"`; (c) the partition
struct has the default partition type's arity, with compatible literals. The manifest is written with
the **current schema** and the **default spec** (`snapshot.rs:242-262`). So files staged under spec 0
cannot be committed after the default moves to spec 1. This matches
`spec_mismatch_at_commit_abandons_and_restages` and "spec evolution only in plan". `check_duplicate`
(default true) reads every manifest of the current snapshot (`snapshot.rs:165+`). Consider
`with_check_duplicate(false)`, since the fence and plan already prevent re-adds.

**Fallback from footers:** `ParquetWriter::parquet_files_to_data_files` exists but is `pub(crate)`
and ignores partitions (`parquet_writer.rs:345-380`, "TODO: support adding to partitioned table").
A footer-rebuild fallback has to be hand-written: read `ParquetMetaData`, compute stats the way
`parquet_to_data_file_builder` (`:384-450`, also `pub(crate)`) does, and set the partition and spec id. Prefer
storing the JSON plus the spec id and schema id.

**Parquet writer determinism:** iceberg's `ParquetWriterBuilder::new(props, schema)` passes the
caller's `WriterProperties` straight to `AsyncArrowWriter::try_new` (`parquet_writer.rs:64-125`,
`528-536`) and never sets `created_by`. The default is
`DEFAULT_CREATED_BY = "parquet-rs version 58.4.1"` (`parquet-58.4.1/src/file/properties.rs:51`).
`AsyncArrowWriter` also embeds the `ARROW:schema` key-value metadata, which is deterministic for a fixed schema.
The footer has no timestamps. cdcell uses `WriterProperties::builder().build()`
(`crates/cdcell/src/iceberg_out.rs:422`). So the same batches give identical bytes **within one parquet
version**. A parquet crate upgrade changes `created_by`, and with it the bytes. `existing_file_different_hash_fails_task`
must therefore treat a hash mismatch across a version change as expected, or cdcell should pin
`set_created_by("cdcell")` to make the bytes independent of the crate version. That pinning is a choice to record under Decisions.

---

## 5. celld v0.6.3 environment variables

| Var | Exists | Semantics | Citation |
|---|---|---|---|
| `CELLD_IDLE_EVICT_S` | yes | Seconds idle before eviction. Unset (the default) disables it. Must be a positive integer. Converted to ms. `evict_idle` evicts candidates idle ≥ that, skips when shedding, and treats it as "a local residency decision, not a handoff" (`begin_eviction(.., false)`). The stop is `StopMode::Evict`, which publishes an **L9 `1-max`** (or a drain L0 for a paged or over-budget cell). The next activation is a new epoch | `actor.rs:2567-2568`, `env_vars.rs:248`, `logic/lib.rs:6107-6142`, `actor.rs:4044`, `ltx_repl.rs:3361-3445`, `cli.rs:354`, `docs/README.md:27-30,662` |
| `CELLD_LTX_COMPACTION_MIN_TXIDS` | yes | The TXID distance `durable_txid - compacted_txid` that queues an L1 fold. Default 256, must be ≥ 2. A fold also triggers at `CELLD_LTX_COMPACTION_MIN_MB` (default 32) pending bytes. It is ignored if `CELLD_LTX_COMPACTION=0` | `ltx_repl.rs:4531-4560`, `4215-4227`, `docs/README.md:672` |
| `CELLD_LTX_PAGED_MIN_MB` | yes | A restore plan of at least this many MiB pages (zero-page marker epoch). Default 256, and `0` pages every chain. Paging also needs `CELLD_LTX_PAGED=1` (the default), **the fleet format gate open** (the `paged_gate open=true` log, set from the lease sample, `main.rs:4558-4570`), a non-facet activation (`allow_paged`), and a remote restore (not a reused local eviction snapshot or clean local reload, `ltx_repl.rs:2380-2393`) | `ltx_repl.rs:697-700`, `1068`, `2440-2466`, `docs/README.md:675-676` |
| `CELLD_DURABILITY` | yes | `fleet` (the default) or `bucket`, and anything else fails startup. `fleet` with a healthy follower: writes are acked by a follower fsync, and the bundle loop owns tiering (**node bundles**; per-cell objects only at drains, folds and L9s; bundling = paced and `bundle_active`, `ltx_repl.rs:4598-4608`). `bucket`: no shipper, so `sync_loop` uploads each capture immediately as a per-cell L0 (n-n, merged after a failed upload) | `main.rs:4232-4243`, `cli.rs:371-376`, `ltx_repl.rs:4590-4620`, `docs/README.md:54-68,666` |

**Implication for the e2e** (the compose runs 3 nodes, celld-a/b/c, fleet by default):
- To produce a **straddle**, use fleet mode with `CELLD_LTX_COMPACTION_MIN_TXIDS=16`, then a drain point
  that is not an L9: a paged cell (`CELLD_LTX_PAGED_MIN_MB=0`, after the gate opens, restored on another node or
  without a local cache), or cleanup. Idle eviction of a small non-paged cell gives an L9, not a drain L0.
- To produce a **paged marker**, set `CELLD_LTX_PAGED_MIN_MB=0`, wait for the `paged_gate open=true` log, and then have a
  takeover or remote restore of a non-facet cell. Check `e<E>/0000/<m>-<m>.ltx` against `is_paged_marker`.
- Freshness of quiet resident cells (design §11.1): with `CELLD_IDLE_EVICT_S` set, the cell's changes
  arrive as an L9 in the old epoch once the cell has been idle that long.
