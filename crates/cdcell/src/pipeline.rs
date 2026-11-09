//! Per-cell processing and commits (design §5, §7, §11).
//!
//! v1 runs one process: units from intake land in per-cell ordered buffers;
//! a confirmation round reads the record a successor must CAS (`own.json`
//! for per-cell files, the writer's lease for bundles) strictly after the
//! unit's GET; confirmed units are applied to the cell's decoder (whose pages
//! are the shadow at the confirmed point) and their events go to the batch;
//! the committer appends the batch to Iceberg and settles the units' messages.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::Value as J;
use tracing::{info, warn};

use cdcell_core::ltx::Unit;
use cdcell_core::schema::{is_internal, TableInfo};
use cdcell_core::Decoder;

use crate::emit::{self, Kind};
use crate::iceberg_out::{self, OutRow, OutValue, Sink};
use crate::source::Store;

pub type MsgId = u64;

#[derive(Clone, Debug)]
pub enum Origin {
    Cell,
    Bundle { node: String, generation: String, log_epoch: u64 },
}

#[derive(Clone, Debug)]
pub struct UnitIn {
    pub cell: String,
    pub epoch: u64,
    pub unit: Unit,
    pub origin: Origin,
    pub msg: Option<MsgId>,
    pub got_at: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Pending,
    Confirmed,
    Suspect,
}

struct Buffered {
    u: UnitIn,
    verdict: Verdict,
}

struct CellState {
    class: String,
    /// Current epoch of the shadow (None: nothing applied yet).
    epoch: Option<u64>,
    confirmed_txid: u64,
    committed: (u64, u64),
    dec: Decoder,
    buffer: BTreeMap<(u64, u64, u64), Buffered>,
    blocked_since: Option<Instant>,
}

impl CellState {
    fn new(cell: &str) -> CellState {
        CellState {
            class: cell.split(':').next().unwrap_or("").to_string(),
            epoch: None,
            confirmed_txid: 0,
            committed: (0, 0),
            dec: Decoder::new(),
            buffer: BTreeMap::new(),
            blocked_since: None,
        }
    }
}

pub struct Settings {
    pub script: String,
    pub ce_type_prefix: String,
    pub gap_timeout: Duration,
}

/// One table's pending rows in the current batch.
struct TableBatch {
    info: TableInfo,
    kv: bool,
    rows: Vec<OutRow>,
}

pub struct Pipeline {
    src: Store,
    state: Store,
    sink: Sink,
    cfg: Settings,
    cells: HashMap<String, CellState>,
    /// message → units not yet settled
    outstanding: HashMap<MsgId, usize>,
    /// messages whose units are all settled, ready to ack
    pub ackable: Vec<MsgId>,
    batch: BTreeMap<String, TableBatch>,
    batch_msgs: Vec<MsgId>,
    batch_progress: BTreeMap<String, (u64, u64, i64)>,
    batch_seq: u64,
    pub stats: Stats,
}

#[derive(Default, Debug, Clone)]
pub struct Stats {
    pub units_in: u64,
    pub applied: u64,
    pub dropped: u64,
    pub events: u64,
    pub commits: u64,
    pub gap_fills: u64,
    pub suspects: u64,
}

impl Pipeline {
    pub fn new(src: Store, state: Store, sink: Sink, cfg: Settings) -> Pipeline {
        Pipeline {
            src,
            state,
            sink,
            cfg,
            cells: HashMap::new(),
            outstanding: HashMap::new(),
            ackable: Vec::new(),
            batch: BTreeMap::new(),
            batch_msgs: Vec::new(),
            batch_progress: BTreeMap::new(),
            batch_seq: 0,
            stats: Stats::default(),
        }
    }

    /// A message produced `n` units (0: ack right away).
    pub fn track(&mut self, msg: MsgId, n: usize) {
        if n == 0 {
            self.ackable.push(msg);
        } else {
            *self.outstanding.entry(msg).or_default() += n;
        }
    }

    fn settle(&mut self, msg: Option<MsgId>) {
        if let Some(m) = msg {
            if let Some(c) = self.outstanding.get_mut(&m) {
                *c -= 1;
                if *c == 0 {
                    self.outstanding.remove(&m);
                    self.ackable.push(m);
                }
            }
        }
    }

    pub fn add(&mut self, u: UnitIn) {
        self.stats.units_in += 1;
        let st = self.cells.entry(u.cell.clone()).or_insert_with(|| CellState::new(&u.cell));
        // Already covered: drop now (design §5.6). Duplicates are settled with
        // the batch so a crash before commit redelivers them.
        if st.epoch.is_some_and(|e| u.epoch < e) || (st.epoch == Some(u.epoch) && u.unit.max_txid <= st.confirmed_txid) {
            self.stats.dropped += 1;
            let m = u.msg;
            self.batch_msgs.extend(m);
            return;
        }
        let key = (u.epoch, u.unit.min_txid, u.unit.max_txid);
        if let Some(old) = st.buffer.insert(key, Buffered { u, verdict: Verdict::Pending }) {
            // same unit seen twice (duplicate notification)
            self.stats.dropped += 1;
            self.batch_msgs.extend(old.u.msg);
        }
    }

    /// One confirmation + apply round over every cell with buffered units.
    pub async fn round(&mut self) -> Result<()> {
        let started = Instant::now();
        let cells: Vec<String> = self.cells.iter().filter(|(_, s)| !s.buffer.is_empty()).map(|(c, _)| c.clone()).collect();
        let mut leases: HashMap<String, Option<J>> = HashMap::new();
        for cell in cells {
            // 1. confirmation reads, started after every pending unit's GET
            let own = self.read_json(&format!("cells/{cell}/own.json")).await?;
            let own_epoch = own.as_ref().and_then(|o| o["epoch"].as_u64());
            let pending_nodes: Vec<String> = self.cells[&cell]
                .buffer
                .values()
                .filter_map(|b| match (&b.u.origin, b.verdict) {
                    (Origin::Bundle { node, .. }, Verdict::Pending) => Some(node.clone()),
                    _ => None,
                })
                .collect();
            for node in pending_nodes {
                if !leases.contains_key(&node) {
                    let l = self.read_json(&format!("nodes/{node}.json")).await?;
                    leases.insert(node, l);
                }
            }
            let st = self.cells.get_mut(&cell).unwrap();
            for b in st.buffer.values_mut().filter(|b| b.verdict == Verdict::Pending && b.u.got_at <= started) {
                b.verdict = match &b.u.origin {
                    Origin::Cell => match own_epoch {
                        Some(e) if e == b.u.epoch => Verdict::Confirmed,
                        Some(e) if e > b.u.epoch => Verdict::Suspect,
                        _ => Verdict::Pending,
                    },
                    Origin::Bundle { node, generation, log_epoch } => {
                        let lease = leases.get(node).cloned().flatten();
                        let gen = lease.as_ref().and_then(|l| l["ownership_index_generation"].as_str().or(l["probe_public_key"].as_str())).unwrap_or("");
                        let open = lease.as_ref().is_some_and(|l| l["log"]["state"] == "open" && l["log"]["epoch"].as_u64().unwrap_or(0) >= *log_epoch);
                        if gen == generation && open {
                            Verdict::Confirmed
                        } else {
                            Verdict::Suspect
                        }
                    }
                };
                if b.verdict == Verdict::Suspect {
                    // Parked in memory; its message is settled with the next
                    // commit so broker redelivery never decides history
                    // (design §5.3; durable parking: M5).
                    self.stats.suspects += 1;
                    self.batch_msgs.extend(b.u.msg.take());
                }
            }
            // 2. apply what is next and confirmed
            self.advance(&cell).await?;
        }
        Ok(())
    }

    /// Applies confirmed units in order; handles epoch openers and gaps.
    async fn advance(&mut self, cell: &str) -> Result<()> {
        loop {
            let st = self.cells.get_mut(cell).unwrap();
            // An opener of a higher epoch (a confirmed TXID-1 snapshot) moves
            // the shadow to that epoch by diff (design §5.4 rule 3, §5.5): it
            // is correct whatever happened to the lower epoch's tail.
            let opener = st
                .buffer
                .iter()
                .filter(|(&(e, min, _), b)| st.epoch.is_none_or(|cur| e > cur) && min == 1 && b.verdict == Verdict::Confirmed)
                .map(|(k, _)| *k)
                .next();
            // A paged-epoch marker `(c+1)-(c+1)` of a higher epoch gives the
            // exact cut c (design §5.4 rule 2, §5.5).
            let marker = st
                .buffer
                .iter()
                .filter(|(&(e, _, _), b)| st.epoch.is_some_and(|cur| e > cur) && b.verdict == Verdict::Confirmed && b.u.unit.is_paged_marker())
                .map(|(k, _)| *k)
                .next();
            let next = st.epoch.and_then(|cur| {
                st.buffer
                    .iter()
                    .find(|(&(e, min, max), _)| e == cur && min <= st.confirmed_txid + 1 && max > st.confirmed_txid)
                    .map(|(k, b)| (*k, b.verdict))
            });
            match (next, opener) {
                (Some((k, Verdict::Confirmed)), _) => {
                    let b = st.buffer.remove(&k).unwrap();
                    self.apply(cell, b.u, Kind::Streamed).await?;
                }
                (Some((_, Verdict::Pending)), _) => return Ok(()),
                (_, Some(k)) => {
                    // Drop everything below the new epoch: covered by the diff.
                    let below: Vec<_> = st.buffer.keys().filter(|&&(e, _, _)| e < k.0).copied().collect();
                    let mut suspects = false;
                    for d in below {
                        let b = st.buffer.remove(&d).unwrap();
                        suspects |= b.verdict != Verdict::Confirmed;
                        self.stats.dropped += 1;
                        self.batch_msgs.extend(b.u.msg);
                    }
                    let st = self.cells.get_mut(cell).unwrap();
                    let b = st.buffer.remove(&k).unwrap();
                    let kind = if suspects && st.epoch.is_some() { Kind::Synthetic } else { Kind::Streamed };
                    self.apply(cell, b.u, kind).await?;
                }
                (_, None) if marker.is_some() => {
                    if !self.paged_cut(cell, marker.unwrap()).await? {
                        return Ok(());
                    }
                }
                _ => {
                    // Blocked: a gap, a suspect tail, or a cell seen mid-epoch.
                    let st = self.cells.get_mut(cell).unwrap();
                    if st.buffer.is_empty() {
                        st.blocked_since = None;
                        return Ok(());
                    }
                    let since = *st.blocked_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= self.cfg.gap_timeout {
                        st.blocked_since = Some(Instant::now());
                        self.fill_gap(cell).await?;
                        continue;
                    }
                    return Ok(());
                }
            }
        }
    }

    /// Rule 2: resolve the current epoch against a paged marker. Returns
    /// false when it cannot proceed yet (missing units, unproven lineage).
    async fn paged_cut(&mut self, cell: &str, marker: (u64, u64, u64)) -> Result<bool> {
        let (new_epoch, m, _) = marker;
        let cut = m - 1;
        let cur = self.cells[cell].epoch.unwrap();
        // The cut must belong to the current epoch: every epoch in between
        // must be empty (the predecessor proof, design §5.5).
        for e in (cur + 1)..new_epoch {
            if !self.src.list(&format!("cells/{cell}/ltx/e{e}/")).await?.is_empty() {
                warn!(cell, cur, new_epoch, intermediate = e, "paged cut belongs to an intermediate epoch; resync needed");
                return Ok(false);
            }
        }
        let confirmed = self.cells[cell].confirmed_txid;
        if cut < confirmed {
            warn!(cell, cut, confirmed, "paged cut below the confirmed point; resync needed");
            return Ok(false);
        }
        // Apply the current epoch's units up to the cut (history whatever
        // their verdict), then drop everything above it (phantoms).
        while self.cells[cell].confirmed_txid < cut {
            let c = self.cells[cell].confirmed_txid;
            let k = self.cells[cell].buffer.keys().find(|&&(e, min, max)| e == cur && min <= c + 1 && max > c && max <= cut).copied();
            match k {
                Some(k) => {
                    let b = self.cells.get_mut(cell).unwrap().buffer.remove(&k).unwrap();
                    self.apply(cell, b.u, Kind::Streamed).await?;
                }
                None => {
                    self.fill_gap(cell).await?;
                    return Ok(false);
                }
            }
        }
        let st = self.cells.get_mut(cell).unwrap();
        let doomed: Vec<_> = st.buffer.keys().filter(|&&(e, _, max)| e < new_epoch && (e != cur || max > cut)).copied().collect();
        for d in doomed {
            let b = st.buffer.remove(&d).unwrap();
            self.stats.dropped += 1;
            self.batch_msgs.extend(b.u.msg);
        }
        let st = self.cells.get_mut(cell).unwrap();
        let b = st.buffer.remove(&marker).unwrap();
        st.epoch = Some(new_epoch);
        st.confirmed_txid = b.u.unit.max_txid;
        self.batch_msgs.extend(b.u.msg);
        info!(cell, cur, new_epoch, cut, "paged epoch: resolved by marker cut");
        Ok(true)
    }

    /// Gap repair (design §7 source 1): LIST the epoch's L0 prefix and fetch
    /// what is missing above the confirmed point. For a cell first seen mid
    /// epoch this fills from TXID 1.
    async fn fill_gap(&mut self, cell: &str) -> Result<()> {
        let st = &self.cells[cell];
        let epoch = st.epoch.or_else(|| st.buffer.keys().next().map(|k| k.0));
        let Some(epoch) = epoch else { return Ok(()) };
        let from = if st.epoch == Some(epoch) { st.confirmed_txid } else { 0 };
        let keys = self.src.list(&format!("cells/{cell}/ltx/e{epoch}/0000/")).await?;
        let mut added = 0;
        for k in keys {
            if let crate::keys::ObjKey::Ltx { max, min, .. } = crate::keys::parse(&k) {
                if max <= from || self.cells[cell].buffer.contains_key(&(epoch, min, max)) {
                    continue;
                }
                if let Some(body) = self.src.get(&k).await? {
                    let unit = cdcell_core::ltx::decode_file(&body)?;
                    self.add(UnitIn { cell: cell.to_string(), epoch, unit, origin: Origin::Cell, msg: None, got_at: Instant::now() });
                    added += 1;
                }
            }
        }
        if added > 0 {
            self.stats.gap_fills += 1;
            info!(cell, epoch, from, added, "gap repair fetched units");
        }
        Ok(())
    }

    async fn apply(&mut self, cell: &str, u: UnitIn, kind: Kind) -> Result<()> {
        let st = self.cells.get_mut(cell).unwrap();
        let changes = st.dec.apply(u.unit.pages, u.unit.commit)?;
        st.epoch = Some(u.epoch);
        st.confirmed_txid = u.unit.max_txid;
        st.blocked_since = None;
        self.stats.applied += 1;
        let rows: Vec<_> = changes.rows.into_iter().filter(|c| !is_internal(&c.table)).collect();
        let class = st.class.clone();
        let ctx = emit::Ctx {
            script: &self.cfg.script,
            class: &class,
            cell,
            epoch: u.epoch,
            txid: u.unit.max_txid,
            capture_ms: u.unit.timestamp_ms,
            now_us: now_us(),
            ce_type_prefix: &self.cfg.ce_type_prefix,
            schema_version: 1,
            kind,
        };
        let dec = &self.cells[cell].dec;
        let (kv, plain): (Vec<_>, Vec<_>) = rows.into_iter().partition(|c| c.table == "_cf_KV");
        let mut out = emit::rows(&ctx, &|t| dec.table_info(t).cloned(), &plain);
        out.extend(emit::kv_rows(&ctx, &kv));
        for (table, row) in out {
            self.stats.events += 1;
            let key = format!("{}/{}", class, table);
            let kvt = table == emit::KV_TABLE;
            let info = if kvt { None } else { dec.table_info(&table).cloned() };
            let entry = self.batch.entry(key).or_insert_with(|| TableBatch {
                info: info.clone().unwrap_or_else(|| emit::kv_info()),
                kv: kvt,
                rows: Vec::new(),
            });
            if let Some(i) = info {
                entry.info = i; // latest schema seen
            }
            entry.rows.push(row);
        }
        self.batch_progress.insert(cell.to_string(), (u.epoch, u.unit.max_txid, u.unit.timestamp_ms));
        self.batch_msgs.extend(u.msg);
        Ok(())
    }

    /// Commits the batch (design §11.2, v1 subset: one snapshot per table,
    /// `_cdcell_progress` last, then the checkpoint, then acks).
    pub async fn commit(&mut self) -> Result<()> {
        if self.batch.is_empty() && self.batch_msgs.is_empty() {
            return Ok(());
        }
        self.batch_seq += 1;
        let seq = self.batch_seq;
        let props = HashMap::from([("cdcell.batch".to_string(), seq.to_string())]);
        let mut events = 0;
        for (key, tb) in std::mem::take(&mut self.batch) {
            let (class, table) = key.split_once('/').unwrap();
            let ns = vec!["cdcell".to_string(), iceberg_out::ident(&self.cfg.script), iceberg_out::ident(class)];
            let schema = if tb.kv { iceberg_out::kv_schema()? } else { iceberg_out::change_schema(&tb.info)? };
            let ident = self
                .sink
                .ensure_table(
                    &ns,
                    // reserved tables keep their name; user tables go through the identifier rule
                    &if tb.kv { table.to_string() } else { iceberg_out::ident(table) },
                    schema,
                    HashMap::from([("cdcell.original-name".to_string(), table.to_string())]),
                )
                .await?;
            events += tb.rows.len();
            self.sink.append(&ident, &tb.rows, props.clone()).await?;
        }
        if !self.batch_progress.is_empty() {
            let ns = vec!["cdcell".to_string(), iceberg_out::ident(&self.cfg.script)];
            let ident = self.sink.ensure_table(&ns, "_cdcell_progress", iceberg_out::progress_schema()?, HashMap::new()).await?;
            let now = now_us();
            let rows: Vec<OutRow> = self
                .batch_progress
                .iter()
                .map(|(cell, (e, t, ms))| {
                    OutRow::from([
                        ("_cell_id".into(), OutValue::Str(cell.clone())),
                        ("_epoch".into(), OutValue::Long(*e as i64)),
                        ("_txid".into(), OutValue::Long(*t as i64)),
                        ("ce_time".into(), OutValue::Ts(ms * 1000)),
                        ("committed_at".into(), OutValue::Ts(now)),
                        ("batch".into(), OutValue::Long(seq as i64)),
                    ])
                })
                .collect();
            self.sink.append(&ident, &rows, props.clone()).await?;
            for (cell, (e, t, _)) in std::mem::take(&mut self.batch_progress) {
                if let Some(st) = self.cells.get_mut(&cell) {
                    st.committed = (e, t);
                }
            }
        }
        self.write_checkpoint(seq).await?;
        for m in std::mem::take(&mut self.batch_msgs) {
            self.settle(Some(m));
        }
        self.stats.commits += 1;
        info!(seq, events, cells = self.cells.len(), stats = ?self.stats, "committed batch");
        Ok(())
    }

    async fn write_checkpoint(&self, seq: u64) -> Result<()> {
        let cells: serde_json::Map<String, J> = self
            .cells
            .iter()
            .map(|(c, s)| (c.clone(), serde_json::json!({"epoch": s.committed.0, "committed_txid": s.committed.1})))
            .collect();
        let doc = serde_json::json!({"version": 1, "last_batch": seq, "cells": cells});
        self.state.put("checkpoint.json", serde_json::to_vec(&doc)?).await
    }

    async fn read_json(&self, key: &str) -> Result<Option<J>> {
        Ok(match self.src.get(key).await? {
            Some(b) => match serde_json::from_slice(&b) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!(key, %e, "unparseable record");
                    None
                }
            },
            None => None,
        })
    }
}

pub fn now_us() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64
}
