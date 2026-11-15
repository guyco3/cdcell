# cdcell batch architecture: implementation plan

Status: audited (2026-11-14), revised.

Implements [`batch-design.md`](batch-design.md) (authoritative). Paths are
relative to this repo unless prefixed `celld/`. Strict red/green TDD: each PR
lists the failing tests written first. Every PR: `scripts/cargo.sh clippy
--all-targets -j 2 -- -D warnings` and `scripts/cargo.sh test -j 2` pass (one
build at a time). Streaming code stays compiling until PR 10. The live tail
is not implemented in v1.

## 0. Findings from the current code

| Area | Current code | Consequence |
|---|---|---|
| Restore chain | `resync.rs:109-161` `plan_chain` returns `None` when an intermediate epoch does not end at the cut; celld `ltx/src/client/epochs.rs:50-80` `continue`s (skips it), skips empty epochs (incl. the newest, `:64`), clips each epoch to `max < above.lo`, and stops at any level's `min == 1` (incl. L9) | port `EpochChain::build` exactly as a pure fn, property-tested against celld-ltx (already a dev-dep) |
| Within-epoch path | `plan_chain` greedy max reach (`resync.rs:131-138`), like celld `calc_restore_plan` (`replica.rs:669-760`) | finest-chain DP is new: reach must equal celld's restore reach; steps ≥ |
| Marker | `cdcell-core/src/ltx.rs:25-30` `is_paged_marker` (zero pages, `wal_size == 0`; celld `ltx/src/db.rs:961-985`) | the only way a fetched object proves a cut; keys never do (bucket-mode L0s are `n-n`) |
| L0 straddles | celld `sync_cell` uploads one merged L0 from `replica.pos()+1` (`celld/ltx_repl.rs:4110,4150-4160`); fleet folds don't `seed_pos` | the StartAfter bound is false; full-L0 fallback (design §2.1) |
| Listing | `source.rs:77-95` `list` has no StartAfter/delimiter | add `list_after` (`list_with_offset`, results sorted), `list_dirs` (`list_with_delimiter`, paginated) |
| File names | `iceberg_out.rs:724-735` random; rows unsorted; overwrite | `b<seq>-t<i>-NNNNN.parquet`, `PutMode::Create`; sort by `__cell_id, __key, __epoch, __txid, __op` |
| Partition spec | hard-coded `day + bucket(16)` `iceberg_out.rs:325-332` | configurable; evolved only in `plan` (iceberg `snapshot.rs:150` rejects non-default spec ids) |
| Staging | `iceberg_out.rs:370-464` captures fast_append at stage time | stage writes data files + serialized `DataFile`s (spec/schema id) only; committer builds the fast_append over the current parent (`capture.rs`); on Conflict rebuild, reuse files |
| Commit errors | `iceberg_out.rs:503-509` stringly | classify by reloading the fence head (design §3.3) |
| Schema registry | `iceberg_out.rs:554-601` property, last writer wins, no schema-id assert | CAS registry in the state prefix; adding an existing name errors (iceberg `update_schema.rs:405`): on 409 recompute `plan_columns`, add only missing |
| Roll forward | `pipeline.rs:1372-1450` via `has_batch` on table snapshots | fence head summary `cdcell.batch`/`cdcell.last-batch` + `commit.json` |
| Lease | `pipeline.rs:1453-1511` | reuse in `batch/state.rs`; renewals; never take unexpired |
| Resync | `pipeline.rs:557-720` `resync_later` + `live_keys` | full image onto restored state (decoder emits the exact diff); `live_keys` only when no state can be restored |
| Assembly | `pipeline.rs:502-552` `assemble`, `:1528` `image_of` | reuse for rebuilds |
| Test clock | `pipeline.rs:1546-1568` | move to `clock.rs` |
| Test kit | `tests/common/mod.rs:353-367` `fold_l1` deletes L0s (contradicts B13), always from TXID 1 | add `fold_l1_keep(cell, from)`, `delete_l0s`, `drain_l0_from(seed)`; old `fold_l1` stays until PR 10 |
| Intake | `run.rs:89-108` `keys_of` | move to `broker/mod.rs::parse_s3_records` |
| emit | `emit.rs:43-51` `Ctx.now_us` → `__ingested_at` | pass `planned_at` |

Design-doc path and naming decisions are applied in the design doc (keys
files, snapshot root, file names, `layout.json`, `commit.json`). Split-mode
lease: `plan` takes it with TTL `CDCELL_RUN_LEASE` (2h), renewed by stage and
commit; the fence guarantees correctness.

## 1. Layout

```
crates/cdcell/src/
  clock.rs          NEW now_us / use_test_clock (from pipeline.rs)
  keys.rs           + ltx_key(), Obj::from_key
  source.rs         + list_after (sorted), list_dirs (paginated), put_create, put_cas, from_url
  emit.rs           Ctx.now_us = planned_at
  resync.rs         keep live_keys/owed_deletes/key_columns/key_values; plan_chain removed in PR 10
  iceberg_out.rs    keep schemas, OutRow/OutValue, to_batch, plan_columns, ident; Sink → lake/rest.rs
  capture.rs        keep
  metrics.rs        + batch metrics (§3)
  batch/
    mod.rs          BatchConfig::from_env (CDCELL_PARTITIONS=16, CDCELL_GRAIN, CDCELL_GAP_LISTS=3,
                    CDCELL_SNAPSHOT_EVERY=8, CDCELL_PARTITION[_<t>], CDCELL_STATE_URL, CDCELL_RUN_INTERVAL=3h,
                    CDCELL_BATCH_{CELLS,BYTES,ROWS}, CDCELL_MEMORY_MB, CDCELL_COMMIT_WAIT, CDCELL_ORPHAN_MIN_AGE)
    obj.rs          Obj, Pos, Cursor                         PURE
    chain.rs        epoch_chain, best_path                   PURE
    plan.rs         CellView → Decision; prove(); start_after_key()   PURE
    partition.rs    partition_of(cell, P)                    PURE
    state.rs        layout, lease, seq alloc, run plan, keys files, commit.json, pending, shards, snapshots
    registry.rs     schema-version CAS counter
    budget.rs       global byte-weighted semaphore
    cell.rs         process_cell: list → GET → own.json → prove → restore → apply → rows
    stage.rs        stage_task
    commit.rs       commit_batch, classify, roll_forward, finish
    planner_step.rs plan: lease, fence, roll forward, evolve specs, seq, drain, keys files, ack
    run.rs          run_once = loop { plan + stage + commit } until no work; every-mode
    repartition.rs  offline doubling
  broker/ mod.rs (trait Broker, Delivery, parse_s3_records, MemBroker), jetstream.rs, sqs.rs
  lake/   mod.rs (trait Lake, TableSpec, StagedFile, BatchCommit, CommitOutcome, FenceHead),
          layout.rs (PURE spec/sort/prefix/name), mem.rs (MemLake), rest.rs (REST impl)
REMOVED in PR 10: pipeline.rs, run.rs (old), intake.rs, out.rs, tests/{resync,fleet,schema}.rs (ported first)
```

Key pure signatures:

```rust
pub struct Pos { pub epoch: u64, pub txid: u64 }          // Ord = total order
pub struct Obj { pub epoch: u64, pub level: u32, pub min: u64, pub max: u64 }
pub struct ChainEpoch { pub epoch: u64, pub lo: u64, pub objs: Vec<Obj> }
pub fn epoch_chain(by_epoch: &BTreeMap<u64, Vec<Obj>>) -> Option<Vec<ChainEpoch>>; // exact EpochChain::build port
pub enum Grain { Finest, Fold }
pub struct Path { pub steps: Vec<Obj>, pub reach: u64 }
/// (a,b) applicable at p iff a <= p+1 <= b; maximize reach, then steps (Finest) / min steps (Fold)
pub fn best_path(objs: &[Obj], from: u64, limit: Option<u64>, grain: Grain) -> Path;

pub enum StateAvail { Snapshot(Pos), Rebuildable, None }
pub struct CellView<'a> { cursor: Option<Pos>, state: StateAvail, objs: &'a [Obj],
    listed: &'a Listed, gap: Option<GapMemo>, parked: Option<ParkReason> }
pub enum ListReq { EpochDirs, Level { epoch: u64, level: u32, start_after: Option<u64> } } // None = full
pub enum Step { Apply{epoch,obj}, Opener{epoch,obj}, Marker{epoch,cut} }
pub enum Base { Cursor, Rebuild(Vec<Obj>), ResyncFromZero(Vec<Obj>) }
pub struct Plan { base: Base, steps: Vec<Step>, dropped: Vec<Obj> }
pub enum ParkReason { Hole, AwaitingOpener, Unproven }
pub enum Decision { Idle, List(Vec<ListReq>), Fetch(Plan), Park(ParkReason) }
pub fn plan_cell(v: &CellView, s: &PlanSettings) -> Decision;
pub struct Header { pages: u32, wal_size: u64 }               // from the fetched LTX header
pub struct Proven { steps: Vec<Step>, head: Option<Pos>, parked: Option<ParkReason>, resync: bool }
/// cut proven only by a fetched zero-page marker or a full listing of E+1 from TXID 0
pub fn prove(plan: &Plan, headers: &BTreeMap<Obj, Header>, listed: &Listed, own_epoch: Option<u64>) -> Proven;
pub fn start_after_key(cell: &str, epoch: u64, boundary: u64) -> String;
pub fn partition_of(cell: &str, p: u32) -> u32;   // sha256(cell)[0..4] BE mod p, p power of two
```

The executor loops `plan_cell → List → plan_cell` at most three times per
run (gap LIST, full-L0 fallback, epoch walk), then `Fetch`; LIST policy stays
in the pure function.

I/O seams: `trait Broker { fetch(max, wait) -> Vec<Delivery{token, keys, size}>; ack(tokens) }`;
`trait Lake { ensure_table; evolve_spec; add_columns(expect_schema_id, cols) -> Applied|Conflict;
write_files(rows pre-sorted, name) -> Vec<StagedFile> (create-only); fence_head; take_fence(expect, new_id, last_batch);
commit_batch -> Ok|Err(raw); live_keys }`. The cell executor takes `live_keys` as a closure.

State prefix (`CDCELL_STATE_URL`):
```
_cdcell/layout.json                         create-only {P}; mismatch → refuse
_cdcell/lease.json                          CAS
_cdcell/batches/<seq:020>.json              run plan, create-only (planned_at, P, tasks, spec ids, fence_expect)
_cdcell/batches/<seq>/p<k>.keys.json        keys + frozen from-cursors, create-only, before the broker ack
_cdcell/batches/<seq>/p<k>.keys.merged      marker: merged into a later seq
_cdcell/batches/<seq>/p<k>.json             staged result (files with spec/schema id), create-only
_cdcell/batches/<seq>/commit.json           create-only {partitions: {k: sha256(p<k>.json)}, fence_new_id}
_cdcell/p<k>/checkpoint.json                ETag CAS {seq, cells{cursor, boundary, snapshot, replay, gap, parked}}
_cdcell/p<k>/pending.json                   ETag CAS {seq, keys}
_cdcell/schemas/<ns.table>/registry.json    CAS counter, fingerprint → version
snapshots/<urlenc(cell)>/<epoch:020>-<txid:020>.zst
```

## 2. PR stack (on top of `batch-design`, PR #7)

### PR 0 spikes (no product code; findings recorded here)
1. **L0 straddle (M8):** kit primitive `Celld::drain_l0_from(seed)` (one merged L0 from a seed position,
   as celld `sync_cell`) and straddle generators; confirm a drain L0 `1-600` over L1s `1-256`, `257-512`,
   and that StartAfter + full-L0 fallback closes every generated gap.
2. **Marker identification:** real celld paged markers have zero pages and `wal_size == 0`; no bucket-mode
   `n-n` L0 does.
3. **add-spec on a rev-5 table** (`add-spec`, `set-default-spec`, `replace-sort-order`) on Lakekeeper 0.15.0.
4. **DataFile JSON across processes** with an evolved schema (partition values, column stats); fallback:
   rebuild from Parquet footers.
5. **Fence shapes: verified on Lakekeeper 0.15.0**, record only: first take `assert-ref-snapshot-id` with
   `snapshot-id: null`; `add-snapshot` whose manifest list need not exist; `set-snapshot-ref` `type: branch`;
   success 200 empty body; stale → 409 `CatalogCommitConflicts`; sequence-number conflict → 409
   `CommitFailedException`.

### PR 1 `batch/1-chain`: pure chain core
Files: `batch/{mod,obj,chain}.rs`, `keys.rs`, dev-dep `proptest`, `tests/chain_props.rs`, kit generators.
Tests first:
- `epoch_chain_single_snapshot_epoch`, `epoch_chain_paged_link_clips_fenced_tail`,
  `epoch_chain_skips_intermediate_epoch_not_ending_at_cut`, `epoch_chain_skips_empty_epochs`,
  `epoch_chain_skips_empty_newest_epoch`, `epoch_chain_stops_at_first_lo_1`, `epoch_chain_l9_counts_as_lo_1`,
  `epoch_chain_none_without_objects`, `epoch_chain_none_when_no_link_reaches_cut`.
- `best_path_prefers_l0s_over_covering_l1` (256 steps Finest / 1 Fold), `best_path_overlapping_merged_l0s`,
  `best_path_straddling_object_applies_from_cursor`, `best_path_drain_l0_over_l1s`,
  `best_path_stops_at_gap_reports_reach`, `best_path_respects_limit`, `best_path_deterministic_tie_break`.
- `tests/chain_props.rs` (in-memory `celld_ltx::ReplicaClient`; generators with straddles and overlaps):
  `prop_epoch_chain_matches_celld`, `prop_best_path_valid_and_maximal` (brute force n ≤ 8),
  `prop_chain_path_reach_equals_celld_restore_plan` (compares reach only when celld returns Ok).
Acceptance: props at 256 cases < 60 s in the VM.

### PR 2 `batch/2-planner`: pure planner and proof
Files: `batch/plan.rs`, `batch/partition.rs`.
Tests first (table-driven `CellView`s): `contiguous_notified_keys_advance_without_list`,
`out_of_order_keys_sorted_before_planning`, `duplicate_keys_deduped`, `late_key_at_or_below_cursor_ignored`,
`late_file_in_older_epoch_with_higher_txid_ignored`, `missing_middle_requests_gap_list` (L1, L9 full; L0
start-after), `gap_not_closed_by_start_after_lists_l0_in_full`, `missing_end_advances_to_last_provable`,
`after_gap_list_plans_through_listed_object`, `gap_list_count_increments_only_for_same_gap`,
`gap_lists_exhausted_parks_hole`, `parked_hole_resumes_on_later_epoch_opener`,
`first_seen_with_txid1_opener_needs_no_list`, `first_seen_without_full_image_lists_epoch_chain`,
`bootstrap_with_empty_owner_epoch_parks`, `state_missing_rebuilds_from_newest_full_image_at_or_below_cursor`,
`epoch_change_without_notified_opener_lists_epochs`, `snapshot_opener_collapses_unproven_tail`,
`old_epoch_objects_proven_when_own_equals_epoch`, `zero_page_marker_header_proves_cut`,
`full_listing_lo_of_next_epoch_proves_cut`, `single_txid_l0_is_not_marker_without_zero_page_header`,
`non_lowest_object_in_new_epoch_never_sets_cut`, `objects_above_cut_never_proven`,
`cut_below_cursor_triggers_resync`, `cursor_epoch_skipped_by_chain_triggers_resync`,
`intermediate_epoch_must_be_listed_before_marker_proves_cut`, `unproven_without_opener_parks_awaiting_opener`,
`own_missing_or_unparseable_proves_nothing`, `released_owner_same_epoch_still_proves`,
`cursor_epoch_gcd_resyncs_from_owner_chain_head`, `start_after_key_excludes_min_at_or_below_boundary`,
`fold_grain_selects_l1_over_l0s`, `partition_of_pinned_values`, `partition_doubling_splits_ranges`.
Acceptance: every row of the design's late/lost table has a named test; no async in plan.rs/chain.rs.

### PR 3 `batch/3-state`: state prefix, snapshots, registry, store layers
Files: `source.rs`, `batch/{state,registry}.rs`, `clock.rs`, dep `zstd`, `tests/common/store.rs`
(`FaultyStore`: op log + injection), `tests/state.rs`.
Tests first: `list_after_skips_keys_at_or_before_and_sorts`, `list_dirs_paginates_all_epochs`,
`layout_written_once_mismatch_refuses_to_run`, `seq_allocation_create_only_one_wins`,
`run_plan_keys_files_commit_json_are_create_only`, `commit_json_hashes_partition_records`,
`checkpoint_shard_cas_round_trip`, `checkpoint_rejects_unknown_version`, `stale_finisher_cannot_regress_checkpoint`
(lower seq never written), `pending_cas_stale_writer_loses`, `batch_caps_from_env`,
`snapshot_round_trip_reproduces_decoder_rows`, `snapshot_key_is_urlencoded_non_nesting`,
`snapshot_gc_deletes_only_previous_key`, `registry_assigns_in_first_sight_order`,
`registry_same_fingerprint_concurrent_gets_one_version`, `registry_retries_on_cas_conflict`,
`lease_acquire_when_expired`, `lease_never_taken_while_unexpired`, `lease_renew_fails_after_takeover`,
`faulty_store_injects_by_op_and_key_glob`, `faulty_store_records_op_order`.

### PR 4 `batch/4-lake-seam`: Lake trait, MemLake, layout
Files: `lake/{mod,layout,mem}.rs`, `iceberg_out.rs` visibility.
Tests first: `parse_partition_spec_variants`, `per_table_setting_overrides_default`,
`sort_rows_total_order_with_op_tiebreak`, `file_name_is_b_seq_t_i`, `spec_evolution_updates_for_rev5_table`
(add-spec, set-default-spec, replace-sort-order), `sort_order_declared_on_create`; MemLake:
`commit_all_tables_or_none`, `stale_fence_rejects_whole_transaction`, `take_fence_copies_last_batch`,
`take_fence_invalidates_older_holder`, `ambiguous_fault_applies_but_errors`,
`conflict_fault_then_rebuild_succeeds_with_same_files`, `write_files_create_only_same_bytes_ok_different_fails`,
`non_default_spec_file_rejected`, `add_existing_column_errors`, `orphan_files_listed`, `expire_snapshots`.

### PR 5 `batch/5-cell`: cell executor
Files: `batch/{cell,budget}.rs`, kit (`fold_l1_keep`, `delete_l0s`, `drain_l0_from`, `zombie_put`, `own_bump`,
fleet `activate` with opener only in a bundle (D15), `history()` validity oracle), `tests/batch_cell.rs`.
Tests first: `bootstrap_emits_r_rows_from_newest_full_image`, `incremental_one_version_per_object`,
`rows_carry_planned_at`, `normal_path_does_no_list`, `own_json_read_after_every_ltx_get`,
`gap_list_finds_missing_object` (exactly 3 LISTs), `drain_l0_straddle_closed_by_full_l0_list`,
`epoch_change_paged_lists_dirs_plus_three_per_walked_epoch`, `paged_cut_from_fetched_marker_header`,
`snapshot_epoch_restart_collapses_tail_into_one_diff`, `fenced_owner_tail_never_emitted`,
`l1_fold_past_cursor_applies_exactly` (no resync, no lake scan), `snapshot_written_after_snapshot_every_objects`,
`paged_cell_snapshotted_on_first_sight_and_each_changed_run`, `paged_cell_snapshot_memory_bounded`,
`restore_from_snapshot_skips_celld_replay`, `rebuild_without_snapshot_from_celld_chain`,
`resync_after_epoch_gc_diffs_restored_state`, `resync_without_state_uses_live_keys_closure`,
`get_error_leaves_cursor_and_pends_keys`, `object_404_after_list_pends_cell`, ported
`dropped_table_rows_are_deleted`, `renamed_table_moves_rows`, batch variants of `streams_through_paged_activation`,
`bootstraps_cell_first_seen_in_paged_epoch`, `restart_inside_paged_epoch`,
`resync_when_owner_epoch_paged_and_committed_epoch_gcd`, `fleet_cell_waits_for_per_cell_object`.

### PR 6 `batch/6-rest-lake`: REST Lake and ITs
Files: `lake/rest.rs`, `tests/lakekeeper_it.rs` (`#[ignore]`, needs `CDCELL_IT_CATALOG`, run with
`RUST_NET=cdcell-e2e_fleet`). Fence: `_cdcell_fence` branch `run`; take and commit send the PR 0 shapes
(summary `cdcell.batch`, `cdcell.last-batch`, `cdcell.fence-owner`).
Tests first (no network): `data_file_json_round_trip_with_partition_and_stats`, `put_mode_create_on_parquet`,
`raw_409_and_5xx_surface_unclassified`; IT: `it_fence_first_take_null_assert`,
`it_fence_snapshot_without_files_returns_200_empty`, `it_stale_assert_ref_rejects_multi_table_commit_409_no_changes`,
`it_sequence_conflict_409_commit_failed`, `it_add_columns_409_then_retry_missing_only`,
`it_add_existing_column_errors`, `it_commit_batch_with_progress_and_fence_then_fence_head_has_batch_summary`,
`it_rev5_table_add_spec_and_sort_order`, `it_data_file_json_across_processes_evolved_schema`.
This IT is the `b-lakekeeper-fence` gate.

### PR 7a `batch/7a-plan`: plan, broker seam, keys files
Files: `batch/planner_step.rs`, `broker/{mod,jetstream}.rs`, `main.rs` (`plan`), `tests/common/batch.rs`
(BatchHarness), `tests/batch_plan.rs`.
Tests first: `non_ltx_keys_dropped`, `parse_s3_records_url_decodes_and_filters_created`,
`mem_broker_redelivers_unacked`, `plan_does_not_take_unexpired_lease`, `fence_take_copies_last_batch`,
`spec_evolution_only_in_plan_before_stage`, `plan_records_spec_ids`, `write_order_seq_then_drain_then_keys_then_ack`,
`broker_acked_only_after_keys_files_durable`, `ack_failure_after_keys_files_keeps_pending`,
`concurrent_plans_one_wins_no_lost_keys`, `two_abandoned_runs_keys_merged_once`,
`keys_files_freeze_from_cursors`, `plan_caps_cells_and_notified_bytes`.

### PR 7b `batch/7b-stage`: stage
Files: `batch/stage.rs`, `main.rs` (`stage --task i`), `tests/batch_stage.rs`.
Tests first: `stage_uses_frozen_cursors_not_live_checkpoint`, `stage_task_retried_after_commit_writes_nothing`,
`stage_noop_when_record_exists`, `stage_rerun_same_plan_identical_files`, `existing_file_different_hash_fails_task`,
`failed_cell_rows_absent_from_staged_files`, `large_cell_respects_global_memory_budget`,
`stage_cap_stops_cells_at_proven_point_rest_pending`, `schema_race_between_stage_tasks`,
`schema_add_409_recomputes_and_adds_only_missing` (incl. `<col>__<type>`), `lease_renewed_during_stage`.

### PR 7c `batch/7c-commit`: commit, roll forward, finish, run loop
Files: `batch/{commit,run}.rs`, `main.rs` (`commit`, `run --once`), `tests/batch_run.rs`.
Tests first: `run_once_end_to_end_type1_equals_oracle`, `commit_json_written_before_send`,
`late_partition_after_commit_not_rolled_forward`, `roll_forward_after_take_finds_last_batch`,
`roll_forward_after_maintain_expired_snapshots`, `roll_forward_idempotent_after_partial_finish`,
`ambiguous_commit_applied_finishes`, `ambiguous_commit_not_applied_resends`,
`ambiguous_then_late_apply_then_resend_409_is_committed`, `fence_takeover_old_committer_fenced`,
`other_409_rebuilds_and_reuses_staged_files`, `spec_mismatch_at_commit_abandons_and_restages`,
`crash_after_plan_keys_carried_to_next_run`, `crash_between_stage_and_commit_replans_new_seq_no_duplicates`,
`crash_after_commit_before_checkpoint_rolls_forward`, `missing_partition_cursor_unchanged_picked_up_next_run`,
`commit_wait_timeout_leaves_partition_for_next_run`, `failed_cell_pending_others_commit`,
`stale_finisher_cannot_regress_checkpoint` (run level), `checkpoint_lost_rebuilt_from_progress`,
`consistent_read_invariant` (after every commit, per cell each table's max `(epoch, txid)` ≤ its progress row),
`outage_backlog_splits_into_sequential_batches`, `scale_out_split_equals_in_process_run`,
`batches_commit_in_seq_order_progress_rows_per_cell`, `multi_run_restart_between_runs_collapses_snapshot_epoch`,
`hole_parks_after_three_runs_and_resumes_on_opener`, `previous_snapshot_deleted_after_checkpoint`,
`lease_renewed_during_commit`.

### PR 7d `batch/7d-harness`: random histories and crashes
Files: `tests/batch_random.rs`, kit generators.
Tests first: `random_celld_histories_valid_and_converge` (64 seeds; generators cover merged drain L0s, L1s
from bundle rows, recovery windows, straddles; validity oracle, no duplicates, final type-1 equals oracle);
`crash_at_every_op_index` (model-based: FaultyStore op log across plan/stage/commit/finish, crash at each
index, rerun to convergence, same invariants).

### PR 8 `batch/8-ops`: SQS, every-mode, maintain, repartition, metrics
Tests first: `sqs_parses_s3_and_eventbridge_shapes`, `sqs_test_event_ignored`, `sqs_ack_deletes_after_durable`
(+ ignored elasticmq IT), `every_mode_runs_on_interval`, `state_cache_bounded_and_hit_skips_snapshot_get`,
`maintain_removes_orphans_of_abandoned_batches_only`, `maintain_concurrent_with_stage_keeps_inflight_files`,
`maintain_respects_age_floor_and_commit_json`, `maintain_never_expires_fence_head`,
`repartition_doubles_moves_shards_and_pending_then_runs`, `repartition_refuses_while_lease_held`,
`metrics_expose_batch_names`, `run_report_written`. IAM policy for the state prefix (`docs/deploy.md` draft).
e2e: compose env for `CELLD_IDLE_EVICT_S`, `CELLD_LTX_COMPACTION_MIN_TXIDS`, a `cdcell-state` bucket;
`lib.sh` `run_once/plan/stage/commit`; gates b6-*.
Metrics: `cdcell_runs_total{result}`, `cdcell_run_duration_seconds{phase}`, `cdcell_batches_committed_total`,
`cdcell_last_batch_seq`, `cdcell_batch_rows_total{table}`, `cdcell_batch_files_total`,
`cdcell_cells_processed_total`, `cdcell_cells_failed_total{reason}`, `cdcell_cell_parked{reason}`,
`cdcell_gap_lists_total{kind}`, `cdcell_s3_requests_total{op,bucket}`, `cdcell_resyncs_total{reason}`,
`cdcell_commit_outcome_total{outcome}`, `cdcell_commit_retries_total{reason}`, `cdcell_fence_takes_total`,
`cdcell_snapshot_puts_total`, `cdcell_snapshot_bytes_total`, `cdcell_memory_inflight_bytes`,
`cdcell_broker_messages_total`, `cdcell_pending_keys`, `cdcell_maintain_orphans_deleted_total`.

### PR 9 `batch/9-gates`: e2e gates, cost, deploy
All b* gates below not delivered earlier; `docs/cost.md` (MinIO request metrics; cells, write rate, restart
rate, run interval; celld `CELLD_LTX_COMPACTION_MIN_TXIDS=16` write-path and takeover cost); deploy image
with a static binary; `docs/deploy.md` (schedulers, IAM/KMS for lake and state prefix, broker setup,
celld settings guidance from design §5). All gates green before PR 10.

### PR 10 `batch/10-remove-streaming`
Delete streaming modules and tests; `cdcell run` = batch; `up.sh` keeps only the `cells/` `.ltx` rule;
switch `all-gates.sh`; delete m2-m5 gates; update design.md (superseded), consumer-guide, e2e README.

## 3. Test kit

- `Celld`: `fold_l1_keep`, `delete_l0s`, `drain_l0_from(seed)`, `zombie_put` (D10), `own_bump`, fleet opener
  only in a bundle (D15), `history()` validity oracle; generators for straddles, merged drain L0s, L1s from
  bundle rows, recovery windows.
- `FaultyStore`: op log; `fail(op, glob, n, kind)` Error/NotFound/PreconditionFailed/Delay; crash at op index.
- `MemBroker`: at-least-once, drop/duplicate/shuffle, redelivery after `abandon()`.
- `MemLake`: per-table sequence, fence branch with `last-batch`, create-only files, spec-id check,
  expire, faults Fenced/Conflict/Ambiguous(late apply)/Reject.
- `BatchHarness`: `enqueue`, `run_once`, `plan/stage/commit/finish_only`, `second_runner`, `advance`,
  `type1`, `assert_no_duplicates`, `assert_versions_valid`, `assert_consistent_read`, `s3_ops`.

## 4. e2e (same docker stack)

| Old | New | PR | Scenario |
|---|---|---|---|
| m2 | b2-single | 9 | single node load; run_once; type-1 == dump |
| m3 | b3-fleet | 9 | 3 nodes, idle evict; run ×3; no duplicates; no bundle GETs |
| m4 | b4-epochs | 9 | pause/stall/zombie/kill; no phantom versions |
| m5 crash | b5-crash | 9 | kill cdcell mid-run ×3; no duplicates |
| m5 lost | b5-gap-list | 9 | drop ~25% of messages incl. last, then write again; gap LISTs > 0 |
| m5 outage | b5-outage | 9 | 3 min heavy load without cdcell; sequential batches catch up |
| m5-l1-resync | b5-l1-straddle | 9 | cursor below a later fold applies exactly; resyncs == 0 |
| m5-epoch-gc | b5-epoch-gc | 9 | committed epoch GC'd → fallback resync |
| m5-bootstrap | b5-bootstrap | 9 | first run bootstraps from newest full image |
| m5-no-shadows | b5-state-prefix | 9 | snapshots only in the state bucket |
| r5 | b-schema-evolution | 9 | as before |
| m6 | b6-metrics/maintenance/iam | 8 | |
| | b-lakekeeper-fence | 6 | PR 6 IT |
| | b-crash-stage-commit, b-fence-takeover, b-schema-race | 9 | |
| | b-freshness | 9 | informational |

## 5. Risks

1. Deterministic Parquet bytes on rerun: not required (create-only compares size+hash; a mismatch fails the
   task and the batch is replanned).
2. `list_with_offset` on MinIO (start-after); results sorted regardless.
3. celld v0.6.3 has `CELLD_IDLE_EVICT_S`, `CELLD_LTX_COMPACTION_MIN_TXIDS` in the e2e image.
4. Snapshot-epoch collapse makes e2e versions coarser: gates must not check per-capture rows.
5. `aws-sdk-sqs` build weight in the 4 GB VM; alternative `reqwest` + `aws-sigv4` behind a feature.
6. Lakekeeper transaction size limits: covered by batch caps.
7. `check_duplicate=true` reads manifests per commit: keep, measure in `cost.md`.
8. Cells that never write again are not exported (expected; `backfill` later).
