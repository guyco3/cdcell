//! Per-cell processing and commits (design §4, §6).
//!
//! v1 runs one process: units from intake land in per-cell ordered buffers;
//! a confirmation round reads the record a successor must CAS (`own.json`
//! for per-cell files, the writer's lease for bundles) strictly after the
//! unit's GET; confirmed units are applied to the cell's decoder (whose pages
//! are the cell state at the confirmed point) and their events go to the batch;
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
    Bundle {
        node: String,
        generation: String,
        log_epoch: u64,
    },
}

#[derive(Clone, Debug)]
pub struct UnitIn {
    pub cell: String,
    pub epoch: u64,
    pub unit: Unit,
    pub origin: Origin,
    pub msg: Option<MsgId>,
    pub got_at: Instant,
    /// Fetched by bootstrap (design §5): its rows are emitted as `r`.
    pub bootstrap: bool,
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
    /// Current epoch of the cell state (None: nothing applied yet).
    epoch: Option<u64>,
    confirmed_txid: u64,
    committed: (u64, u64),
    dec: Decoder,
    /// After a restart: rebuild the cell state from the celld bucket at the
    /// committed point before applying anything (design §5, §6).
    rebuild: bool,
    /// Units up to this TXID re-emit a window already committed (the rebuild
    /// could only reach an earlier point): emitted as synthetic events.
    synthetic_until: u64,
    /// Capture time of the oldest applied-but-uncommitted unit.
    uncommitted_since: Option<i64>,
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
            rebuild: false,
            synthetic_until: 0,
            uncommitted_since: None,
            buffer: BTreeMap::new(),
            blocked_since: None,
        }
    }
}

pub struct Settings {
    pub script: String,
    pub gap_timeout: Duration,
}

/// One table's pending rows in the current batch.
struct TableBatch {
    /// Latest schema seen for the table (None for the KV table).
    info: Option<TableInfo>,
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
    lease_epoch: u64,
    lease_version: Option<object_store::UpdateVersion>,
    owner: String,
    lease_ttl: Duration,
    processed_bundles: std::collections::HashSet<String>,
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
    pub reconciled_bundles: u64,
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
            lease_epoch: 0,
            lease_version: None,
            owner: format!("cdcell-{}-{}", std::process::id(), now_us()),
            lease_ttl: Duration::from_secs(30),
            processed_bundles: std::collections::HashSet::new(),
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
        crate::metrics::inc(&crate::metrics::M.units);
        let st = self
            .cells
            .entry(u.cell.clone())
            .or_insert_with(|| CellState::new(&u.cell));
        // Already covered: drop now (design §4). Duplicates are settled with
        // the batch so a crash before commit redelivers them.
        if !st.rebuild
            && (st.epoch.is_some_and(|e| u.epoch < e)
                || (st.epoch == Some(u.epoch) && u.unit.max_txid <= st.confirmed_txid))
        {
            self.stats.dropped += 1;
            crate::metrics::inc(&crate::metrics::M.duplicates_dropped);
            let m = u.msg;
            self.batch_msgs.extend(m);
            return;
        }
        let key = (u.epoch, u.unit.min_txid, u.unit.max_txid);
        if let Some(old) = st.buffer.insert(
            key,
            Buffered {
                u,
                verdict: Verdict::Pending,
            },
        ) {
            // same unit seen twice (duplicate notification)
            self.stats.dropped += 1;
            self.batch_msgs.extend(old.u.msg);
        }
    }

    /// One confirmation + apply round over every cell with buffered units.
    pub async fn round(&mut self) -> Result<()> {
        let started = Instant::now();
        let cells: Vec<String> = self
            .cells
            .iter()
            .filter(|(_, s)| !s.buffer.is_empty())
            .map(|(c, _)| c.clone())
            .collect();
        let mut leases: HashMap<String, Option<J>> = HashMap::new();
        for cell in cells {
            // 1. confirmation reads, started after every pending unit's GET
            crate::metrics::inc(&crate::metrics::M.confirm_reads);
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
                if let std::collections::hash_map::Entry::Vacant(slot) = leases.entry(node) {
                    crate::metrics::inc(&crate::metrics::M.confirm_reads);
                    let l = self
                        .read_json(&format!("nodes/{}.json", slot.key()))
                        .await?;
                    slot.insert(l);
                }
            }
            let st = self.cells.get_mut(&cell).unwrap();
            for b in st
                .buffer
                .values_mut()
                .filter(|b| b.verdict == Verdict::Pending && b.u.got_at <= started)
            {
                b.verdict = match &b.u.origin {
                    Origin::Cell => match own_epoch {
                        Some(e) if e == b.u.epoch => Verdict::Confirmed,
                        Some(e) if e > b.u.epoch => Verdict::Suspect,
                        _ => Verdict::Pending,
                    },
                    Origin::Bundle {
                        node,
                        generation,
                        log_epoch,
                    } => {
                        let lease = leases.get(node).cloned().flatten();
                        let gen = lease
                            .as_ref()
                            .and_then(|l| {
                                l["ownership_index_generation"]
                                    .as_str()
                                    .or(l["probe_public_key"].as_str())
                            })
                            .unwrap_or("");
                        let open = lease.as_ref().is_some_and(|l| {
                            l["log"]["state"] == "open"
                                && l["log"]["epoch"].as_u64().unwrap_or(0) >= *log_epoch
                        });
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
                    // (design §4; durable parking: M5).
                    self.stats.suspects += 1;
                    self.batch_msgs.extend(b.u.msg.take());
                }
            }
            // 2. apply what is next and confirmed
            self.advance(&cell).await?;
        }
        self.update_gauges();
        Ok(())
    }

    /// The cells with the oldest pending work (why the watermark lags).
    fn log_laggards(&self) {
        let now_ms = now_us() / 1000;
        let mut v: Vec<(i64, &String, &CellState)> = self
            .cells
            .iter()
            .filter_map(|(c, st)| {
                st.buffer
                    .values()
                    .map(|b| b.u.unit.timestamp_ms)
                    .chain(st.uncommitted_since)
                    .min()
                    .map(|t| (now_ms - t, c, st))
            })
            .collect();
        v.sort_by_key(|(age, _, _)| -age);
        for (age, cell, st) in v.into_iter().take(3).filter(|(age, _, _)| *age > 60_000) {
            let units: Vec<String> = st
                .buffer
                .iter()
                .take(4)
                .map(|(k, b)| format!("e{}:{}-{}:{:?}", k.0, k.1, k.2, b.verdict))
                .collect();
            warn!(cell, age_s = age / 1000, epoch = ?st.epoch, confirmed = st.confirmed_txid, buffered = st.buffer.len(), ?units, blocked = st.blocked_since.is_some(), "lagging cell");
        }
    }

    /// Rebuilds a restored cell's state from the celld bucket (design §5):
    /// a base starting at TXID 1 (L0 snapshot, L1 fold, L9), extended by
    /// contiguous objects up to the committed TXID. Candidates: the epoch's
    /// per-cell objects at every level and buffered units (live bundles).
    /// Returns false when no base is available yet.
    async fn rebuild(&mut self, cell: &str) -> Result<bool> {
        let (epoch, committed) = self.cells[cell].committed;
        let all = self
            .src
            .list(&format!("cells/{cell}/ltx/e{epoch}/"))
            .await?;
        let keys: Vec<String> = all.iter().filter(|k| matches!(crate::keys::parse(k), crate::keys::ObjKey::Ltx { max, .. } if max <= committed)).cloned().collect();
        let mut cands: Vec<Unit> = self.fetch_units(keys).await?;
        let st = self.cells.get_mut(cell).unwrap();
        cands.extend(
            st.buffer
                .iter()
                .filter(|(&(e, _, max), _)| e == epoch && max <= committed)
                .map(|(_, b)| b.u.unit.clone()),
        );
        let Some(base) = cands
            .iter()
            .filter(|u| u.min_txid == 1)
            .max_by_key(|u| u.max_txid)
            .cloned()
        else {
            let since = *st.blocked_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= self.cfg.gap_timeout {
                // celld folded the history past the committed point (L1)
                // and the bundles are gone: resync from the next full image.
                let later = all
                    .iter()
                    .filter_map(|k| match crate::keys::parse(k) {
                        crate::keys::ObjKey::Ltx { min: 1, max, .. } if max > committed => {
                            Some((max, k.clone()))
                        }
                        _ => None,
                    })
                    .min_by_key(|(m, _)| *m);
                if let Some((_, k)) = later {
                    if let Some(u) = self.fetch_units(vec![k]).await?.pop() {
                        self.resync_later(cell, epoch, committed, u).await?;
                        return Ok(true);
                    }
                }
                // The committed epoch itself may be gone: celld's epoch GC
                // (opt-in, CELLD_LTX_RETENTION_SECS) deletes prefixes below
                // the restore base. Resync from the newest full image in the
                // owner's current epoch: own.json names it, so it is history
                // (phantoms only live in fenced owners' older epochs).
                let own_epoch = self
                    .read_json(&format!("cells/{cell}/own.json"))
                    .await?
                    .and_then(|j| j.get("epoch").and_then(|e| e.as_u64()));
                if let Some(e) = own_epoch.filter(|&e| e > epoch) {
                    let newest = self
                        .src
                        .list(&format!("cells/{cell}/ltx/e{e}/"))
                        .await?
                        .into_iter()
                        .filter_map(|k| match crate::keys::parse(&k) {
                            crate::keys::ObjKey::Ltx { min: 1, max, .. } => Some((max, k)),
                            _ => None,
                        })
                        .max_by_key(|(m, _)| *m);
                    if let Some((_, k)) = newest {
                        if let Some(u) = self.fetch_units(vec![k]).await?.pop() {
                            self.resync_later(cell, e, committed, u).await?;
                            return Ok(true);
                        }
                    }
                }
                self.cells.get_mut(cell).unwrap().blocked_since = Some(Instant::now());
                self.reconcile_bundles().await?; // the base may be a live bundle row
            }
            return Ok(false);
        };
        let page_size = base.page_size as usize;
        let mut pages: HashMap<u32, Vec<u8>> = HashMap::new();
        let (mut cur, mut commit) = (base.max_txid, base.commit);
        for (p, d) in base.pages {
            pages.insert(p, d);
        }
        loop {
            let next = cands
                .iter()
                .filter(|u| u.min_txid <= cur + 1 && u.max_txid > cur && u.max_txid <= committed)
                .max_by_key(|u| u.max_txid);
            let Some(n) = next else { break };
            for (p, d) in &n.pages {
                pages.insert(*p, d.clone());
            }
            cur = n.max_txid;
            commit = n.commit;
        }
        pages.retain(|&p, _| p <= commit);
        let usable = page_size - usize::from(pages.get(&1).map_or(0, |p| p[20]));
        let image = cdcell_core::btree::Pages {
            page_size,
            usable,
            commit,
            pages,
        }
        .to_image();
        let st = self.cells.get_mut(cell).unwrap();
        st.dec = Decoder::from_image(&image)?;
        st.epoch = Some(epoch);
        st.confirmed_txid = cur;
        st.rebuild = false;
        st.blocked_since = None;
        st.synthetic_until = if cur < committed { committed } else { 0 };
        crate::metrics::inc(&crate::metrics::M.rebuilds);
        info!(
            cell,
            epoch,
            committed,
            rebuilt_to = cur,
            "cell state rebuilt from the celld bucket"
        );
        Ok(true)
    }

    /// Resync from a later full image (design §6): the image's rows as
    /// updates, plus deletes for keys live in the cell's change log but gone
    /// from the image; one net change at the image's (epoch, txid).
    async fn resync_later(
        &mut self,
        cell: &str,
        epoch: u64,
        committed: u64,
        u: Unit,
    ) -> Result<()> {
        use cdcell_core::{Op, RowChange};
        let page_size = u.page_size as usize;
        let pages: HashMap<u32, Vec<u8>> = u
            .pages
            .into_iter()
            .filter(|(p, _)| *p <= u.commit)
            .collect();
        let usable = page_size - usize::from(pages.get(&1).map_or(0, |p| p[20]));
        let image = cdcell_core::btree::Pages {
            page_size,
            usable,
            commit: u.commit,
            pages,
        }
        .to_image();
        let dec = Decoder::from_image(&image)?;
        let class = self.cells[cell].class.clone();
        let ns = vec![
            "cdcell".to_string(),
            iceberg_out::ident(&self.cfg.script),
            iceberg_out::ident(&class),
        ];
        let ctx = emit::Ctx {
            cell,
            epoch,
            txid: u.max_txid,
            capture_ms: u.timestamp_ms,
            now_us: now_us(),
            kind: Kind::Synthetic,
        };
        let infos: Vec<cdcell_core::schema::TableInfo> = dec
            .tables()
            .filter(|t| t.name == "_cf_KV" || !is_internal(&t.name))
            .cloned()
            .collect();
        let mut out: Vec<(String, iceberg_out::OutRow)> = Vec::new();
        let mut deletes = 0usize;
        for info in &infos {
            let kv = info.name == "_cf_KV";
            let changes: Vec<RowChange> = dec
                .rows(&info.name)?
                .into_values()
                .map(|(key, row)| RowChange {
                    table: info.name.clone(),
                    op: Op::Update,
                    key,
                    before: None,
                    after: Some(row),
                })
                .collect();
            let emitted = if kv {
                emit::kv_rows(&ctx, &changes)
            } else {
                emit::rows(&ctx, &|t| dec.table_info(t).cloned(), &changes)
            };
            let image_keys: std::collections::BTreeSet<String> = emitted
                .iter()
                .filter_map(|(_, r)| match r.get("__key") {
                    Some(iceberg_out::OutValue::Str(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            let (table, name, fp) = if kv {
                (
                    emit::KV_TABLE.to_string(),
                    emit::KV_TABLE.to_string(),
                    "kv-v1".to_string(),
                )
            } else {
                (
                    info.name.clone(),
                    iceberg_out::ident(&info.name),
                    emit::fingerprint(info),
                )
            };
            let live = self.sink.live_keys(&ns, &name, cell).await?;
            for k in crate::resync::owed_deletes(&live, &image_keys) {
                let vals = crate::resync::key_values(&k);
                let cols: Vec<(String, iceberg_out::OutValue)> = if kv {
                    vals.into_iter()
                        .last()
                        .flatten()
                        .map(|v| vec![("k".to_string(), v)])
                        .unwrap_or_default()
                } else {
                    info.pk
                        .iter()
                        .zip(vals)
                        .filter_map(|(&i, v)| Some((emit::out_col(&info.columns[i].name), v?)))
                        .collect()
                };
                out.push((table.clone(), emit::delete_row(&ctx, &k, &fp, cols)));
                deletes += 1;
            }
            out.extend(emitted);
        }
        for (table, row) in out {
            self.stats.events += 1;
            crate::metrics::inc(&crate::metrics::M.events);
            let kvt = table == emit::KV_TABLE;
            let info = if kvt {
                None
            } else {
                dec.table_info(&table).cloned()
            };
            let entry = self
                .batch
                .entry(format!("{class}/{table}"))
                .or_insert_with(|| TableBatch {
                    info: None,
                    kv: kvt,
                    rows: Vec::new(),
                });
            if info.is_some() {
                entry.info = info;
            }
            entry.rows.push(row);
        }
        self.batch_progress
            .insert(cell.to_string(), (epoch, u.max_txid, u.timestamp_ms));
        let st = self.cells.get_mut(cell).unwrap();
        st.dec = dec;
        st.epoch = Some(epoch);
        st.confirmed_txid = u.max_txid;
        st.rebuild = false;
        st.blocked_since = None;
        st.synthetic_until = 0;
        st.uncommitted_since.get_or_insert(u.timestamp_ms);
        crate::metrics::inc(&crate::metrics::M.rebuilds);
        warn!(
            cell,
            epoch,
            committed,
            resynced_to = u.max_txid,
            deletes,
            "committed point not reconstructible: resynced from a later full image"
        );
        Ok(())
    }

    /// Gap, parking and watermark-age gauges (design §9).
    fn update_gauges(&self) {
        let now_ms = now_us() / 1000;
        let mut ages: Vec<f64> = Vec::new();
        let (mut gaps, mut parked) = (0u64, 0u64);
        let mut parked_age: f64 = 0.0;
        for st in self.cells.values() {
            if st.blocked_since.is_some() {
                gaps += 1;
            }
            // Parked cells (suspects awaiting a later epoch's opener) are
            // tracked apart from the watermark (design §9).
            if st.buffer.values().any(|b| b.verdict == Verdict::Suspect) {
                parked += 1;
                if let Some(t) = st.buffer.values().map(|b| b.u.unit.timestamp_ms).min() {
                    parked_age = parked_age.max(((now_ms - t).max(0) as f64) / 1000.0);
                }
                continue;
            }
            let oldest = st
                .buffer
                .values()
                .map(|b| b.u.unit.timestamp_ms)
                .chain(st.uncommitted_since)
                .min();
            if let Some(t) = oldest {
                ages.push(((now_ms - t).max(0) as f64) / 1000.0);
            }
        }
        ages.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let max = ages.last().copied().unwrap_or(0.0);
        let p99 = if ages.is_empty() {
            0.0
        } else {
            ages[((ages.len() as f64 * 0.99).ceil() as usize).saturating_sub(1)]
        };
        crate::metrics::set(&crate::metrics::M.open_gaps, gaps as f64);
        crate::metrics::set(&crate::metrics::M.parked_cells, parked as f64);
        crate::metrics::set(&crate::metrics::M.age_max, max);
        crate::metrics::set(&crate::metrics::M.age_p99, p99);
        crate::metrics::set(&crate::metrics::M.parked_age_max, parked_age);
    }

    /// Applies confirmed units in order; handles epoch openers and gaps.
    async fn advance(&mut self, cell: &str) -> Result<()> {
        if self.cells[cell].rebuild && !self.rebuild(cell).await? {
            return Ok(()); // no base yet: wait for gap repair / reconciliation
        }
        loop {
            // Units the confirmed point has passed are duplicates now (e.g. L0
            // files inside an applied L1 fold): settle and drop them.
            let st = self.cells.get_mut(cell).unwrap();
            if let Some(cur) = st.epoch {
                let covered: Vec<_> = st
                    .buffer
                    .keys()
                    .filter(|&&(e, _, max)| e < cur || (e == cur && max <= st.confirmed_txid))
                    .copied()
                    .collect();
                for k in covered {
                    let b = st.buffer.remove(&k).unwrap();
                    self.stats.dropped += 1;
                    crate::metrics::inc(&crate::metrics::M.duplicates_dropped);
                    self.batch_msgs.extend(b.u.msg);
                }
            }
            let st = self.cells.get_mut(cell).unwrap();
            // An opener of a higher epoch (a confirmed TXID-1 snapshot) moves
            // the cell state to that epoch by diff (design §4): it
            // is correct whatever happened to the lower epoch's tail.
            let opener = st
                .buffer
                .iter()
                .filter(|(&(e, min, _), b)| {
                    st.epoch.is_none_or(|cur| e > cur)
                        && min == 1
                        && b.verdict == Verdict::Confirmed
                })
                .map(|(k, _)| *k)
                .next();
            // A paged-epoch marker `(c+1)-(c+1)` of a higher epoch gives the
            // exact cut c (design §4).
            let marker = st
                .buffer
                .iter()
                .filter(|(&(e, _, _), b)| {
                    st.epoch.is_some_and(|cur| e > cur)
                        && b.verdict == Verdict::Confirmed
                        && b.u.unit.is_paged_marker()
                })
                .map(|(k, _)| *k)
                .next();
            let next = st.epoch.and_then(|cur| {
                st.buffer
                    .iter()
                    .find(|(&(e, min, max), _)| {
                        e == cur && min <= st.confirmed_txid + 1 && max > st.confirmed_txid
                    })
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
                    let below: Vec<_> = st
                        .buffer
                        .keys()
                        .filter(|&&(e, _, _)| e < k.0)
                        .copied()
                        .collect();
                    let mut suspects = false;
                    for d in below {
                        let b = st.buffer.remove(&d).unwrap();
                        suspects |= b.verdict != Verdict::Confirmed;
                        self.stats.dropped += 1;
                        crate::metrics::inc(&crate::metrics::M.phantoms_dropped);
                        self.batch_msgs.extend(b.u.msg);
                    }
                    let st = self.cells.get_mut(cell).unwrap();
                    let b = st.buffer.remove(&k).unwrap();
                    let kind = if b.u.bootstrap && st.epoch.is_none() {
                        Kind::Bootstrap
                    } else if suspects && st.epoch.is_some() {
                        Kind::Synthetic
                    } else {
                        Kind::Streamed
                    };
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
        // must be empty (the predecessor proof, design §4).
        for e in (cur + 1)..new_epoch {
            if !self
                .src
                .list(&format!("cells/{cell}/ltx/e{e}/"))
                .await?
                .is_empty()
            {
                warn!(
                    cell,
                    cur,
                    new_epoch,
                    intermediate = e,
                    "paged cut belongs to an intermediate epoch; resync needed"
                );
                return Ok(false);
            }
        }
        let confirmed = self.cells[cell].confirmed_txid;
        if cut < confirmed {
            warn!(
                cell,
                cut, confirmed, "paged cut below the confirmed point; resync needed"
            );
            return Ok(false);
        }
        // Apply the current epoch's units up to the cut (history whatever
        // their verdict), then drop everything above it (phantoms).
        while self.cells[cell].confirmed_txid < cut {
            let c = self.cells[cell].confirmed_txid;
            let k = self.cells[cell]
                .buffer
                .keys()
                .find(|&&(e, min, max)| e == cur && min <= c + 1 && max > c && max <= cut)
                .copied();
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
        let doomed: Vec<_> = st
            .buffer
            .keys()
            .filter(|&&(e, _, max)| e < new_epoch && (e != cur || max > cut))
            .copied()
            .collect();
        for d in doomed {
            let b = st.buffer.remove(&d).unwrap();
            self.stats.dropped += 1;
            crate::metrics::inc(&crate::metrics::M.phantoms_dropped);
            self.batch_msgs.extend(b.u.msg);
        }
        let st = self.cells.get_mut(cell).unwrap();
        let b = st.buffer.remove(&marker).unwrap();
        st.epoch = Some(new_epoch);
        st.confirmed_txid = b.u.unit.max_txid;
        self.batch_msgs.extend(b.u.msg);
        info!(
            cell,
            cur, new_epoch, cut, "paged epoch: resolved by marker cut"
        );
        Ok(true)
    }

    /// Gap repair and bootstrap (design §4, §5). Source 1: the per-cell
    /// prefix, every level (L0, L1 folds and L9 snapshots carry final page
    /// images at their max TXID). For a cell never applied, or stuck below
    /// its current epoch, the target is the epoch named by `own.json` and the
    /// fetch includes its base (an object starting at TXID 1). Source 2:
    /// live bundles.
    async fn fill_gap(&mut self, cell: &str) -> Result<()> {
        crate::metrics::inc(&crate::metrics::M.confirm_reads);
        let own_epoch = self
            .read_json(&format!("cells/{cell}/own.json"))
            .await?
            .and_then(|o| o["epoch"].as_u64());
        let st = &self.cells[cell];
        let (epoch, from, boot) = match (st.epoch, own_epoch) {
            (Some(cur), Some(own)) if own > cur => (own, 0, true),
            (Some(cur), _) => (cur, st.confirmed_txid, false),
            (None, Some(own)) => (own, 0, true),
            (None, None) => match st.buffer.keys().next() {
                Some(k) => (k.0, 0, true),
                None => return Ok(()),
            },
        };
        let boot = boot && st.epoch.is_none();
        let keys = self
            .src
            .list(&format!("cells/{cell}/ltx/e{epoch}/"))
            .await?;
        let wanted: Vec<String> = keys
            .into_iter()
            .filter(|k| match crate::keys::parse(k) {
                crate::keys::ObjKey::Ltx { max, min, .. } => {
                    max > from && !self.cells[cell].buffer.contains_key(&(epoch, min, max))
                }
                _ => false,
            })
            .collect();
        let units = self.fetch_units(wanted).await?;
        let added = units.len();
        for unit in units {
            self.add(UnitIn {
                cell: cell.to_string(),
                epoch,
                unit,
                origin: Origin::Cell,
                msg: None,
                got_at: Instant::now(),
                bootstrap: boot,
            });
        }
        if added > 0 {
            self.stats.gap_fills += 1;
            info!(
                cell,
                epoch,
                from,
                added,
                bootstrap = boot,
                "gap repair fetched units"
            );
        } else {
            // fleet mode: per-cell prefixes can be empty; read live bundles
            self.reconcile_bundles().await?;
        }
        Ok(())
    }

    /// GET + decode LTX objects, 16 at a time (catch-up of a whole fleet's
    /// cells must not proceed one object at a time).
    async fn fetch_units(&self, keys: Vec<String>) -> Result<Vec<Unit>> {
        use futures::StreamExt;
        let src = self.src.clone();
        let results: Vec<Result<Option<Unit>>> = futures::stream::iter(keys)
            .map(|k| {
                let src = src.clone();
                async move {
                    Ok(match src.get(&k).await? {
                        Some(body) => Some(cdcell_core::ltx::decode_file(&body)?),
                        None => None,
                    })
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;
        let mut out = Vec::new();
        for r in results {
            if let Some(u) = r? {
                out.push(u);
            }
        }
        Ok(out)
    }

    /// A bundle already processed (from a notification or reconciliation).
    pub fn mark_bundle(&mut self, key: &str) -> bool {
        self.processed_bundles.insert(key.to_string())
    }

    /// Gap source 2 and tail reconciliation (design §4): GET every bundle
    /// not yet processed. Bundles of live sessions stay until per-cell
    /// objects cover them, so between both sources every TXID's final state
    /// is available.
    pub async fn reconcile_bundles(&mut self) -> Result<()> {
        let keys = self.src.list("log/").await?;
        let mut found = 0;
        let mut live: HashMap<(String, String), bool> = HashMap::new();
        for key in keys {
            let crate::keys::ObjKey::Bundle {
                node,
                generation,
                log_epoch,
                ..
            } = crate::keys::parse(&key)
            else {
                continue;
            };
            if self.processed_bundles.contains(&key) {
                continue;
            }
            // Only live sessions: a sealed or replaced session's rows are in
            // per-cell prefixes after recovery (design §4, §5).
            let sess = (node.clone(), generation.clone());
            if !live.contains_key(&sess) {
                crate::metrics::inc(&crate::metrics::M.confirm_reads);
                let lease = self.read_json(&format!("nodes/{node}.json")).await?;
                let ok = lease.is_some_and(|l| {
                    l["ownership_index_generation"]
                        .as_str()
                        .or(l["probe_public_key"].as_str())
                        == Some(generation.as_str())
                        && l["log"]["state"] == "open"
                });
                live.insert(sess.clone(), ok);
            }
            if !live[&sess] {
                continue;
            }
            self.mark_bundle(&key);
            let Some(body) = self.src.get(&key).await? else {
                continue;
            };
            let got_at = Instant::now();
            for r in cdcell_core::ltx::decode_bundle(&body)? {
                self.add(UnitIn {
                    cell: r.cell,
                    epoch: r.cell_epoch,
                    unit: r.unit,
                    origin: Origin::Bundle {
                        node: node.clone(),
                        generation: generation.clone(),
                        log_epoch,
                    },
                    msg: None,
                    got_at,
                    bootstrap: false,
                });
            }
            found += 1;
        }
        if found > 0 {
            self.stats.reconciled_bundles += found;
            crate::metrics::add(&crate::metrics::M.reconciled_bundles, found);
            info!(found, "bundle reconciliation fetched bundles");
        }
        Ok(())
    }

    async fn apply(&mut self, cell: &str, u: UnitIn, kind: Kind) -> Result<()> {
        let st = self.cells.get_mut(cell).unwrap();
        let kind = if st.synthetic_until > 0
            && u.unit.min_txid <= st.synthetic_until
            && kind == Kind::Streamed
        {
            Kind::Synthetic
        } else {
            kind
        };
        if st.synthetic_until > 0 && u.unit.max_txid >= st.synthetic_until {
            st.synthetic_until = 0;
        }
        let changes = st.dec.apply(u.unit.pages, u.unit.commit)?;
        st.epoch = Some(u.epoch);
        st.confirmed_txid = u.unit.max_txid;
        st.blocked_since = None;
        st.uncommitted_since.get_or_insert(u.unit.timestamp_ms);
        self.stats.applied += 1;
        let rows: Vec<_> = changes
            .rows
            .into_iter()
            .filter(|c| !is_internal(&c.table))
            .collect();
        let class = st.class.clone();
        let ctx = emit::Ctx {
            cell,
            epoch: u.epoch,
            txid: u.unit.max_txid,
            capture_ms: u.unit.timestamp_ms,
            now_us: now_us(),
            kind,
        };
        let dec = &self.cells[cell].dec;
        let (kv, plain): (Vec<_>, Vec<_>) = rows.into_iter().partition(|c| c.table == "_cf_KV");
        let mut out = emit::rows(&ctx, &|t| dec.table_info(t).cloned(), &plain);
        out.extend(emit::kv_rows(&ctx, &kv));
        for (table, row) in out {
            self.stats.events += 1;
            crate::metrics::inc(&crate::metrics::M.events);
            let key = format!("{}/{}", class, table);
            let kvt = table == emit::KV_TABLE;
            let info = if kvt {
                None
            } else {
                dec.table_info(&table).cloned()
            };
            let entry = self.batch.entry(key).or_insert_with(|| TableBatch {
                info: None,
                kv: kvt,
                rows: Vec::new(),
            });
            if info.is_some() {
                entry.info = info; // latest schema seen
            }
            entry.rows.push(row);
        }
        self.batch_progress.insert(
            cell.to_string(),
            (u.epoch, u.unit.max_txid, u.unit.timestamp_ms),
        );
        self.batch_msgs.extend(u.msg);
        Ok(())
    }

    /// Commits the batch (design §6):
    /// evolve schemas → stage tables → create-only manifest → one atomic
    /// multi-table commit → checkpoint → acks. After a successful commit
    /// every touched cell's state is exactly at its committed point.
    pub async fn commit(&mut self) -> Result<()> {
        if self.batch.is_empty() && self.batch_progress.is_empty() {
            for m in std::mem::take(&mut self.batch_msgs) {
                self.settle(Some(m));
            }
            return Ok(());
        }
        self.renew_lease().await?;
        let commit_started = Instant::now();
        let seq = self.batch_seq + 1;
        let props = HashMap::from([
            ("cdcell.batch".to_string(), seq.to_string()),
            (
                "cdcell.lease-epoch".to_string(),
                self.lease_epoch.to_string(),
            ),
        ]);
        // 1. committed points per touched cell (no copy of celld state: a
        //    restart rebuilds cell state from the celld bucket)
        let mut cells_doc = serde_json::Map::new();
        for (cell, (e, t, _)) in &self.batch_progress {
            cells_doc.insert(cell.clone(), serde_json::json!({"epoch": e, "txid": t}));
        }
        // 2. stage every table (data files written, commit captured)
        let mut staged = Vec::new();
        let mut tables = Vec::new();
        let mut events = 0;
        for (key, tb) in std::mem::take(&mut self.batch) {
            let (class, table) = key.split_once('/').unwrap();
            let ns = vec![
                "cdcell".to_string(),
                iceberg_out::ident(&self.cfg.script),
                iceberg_out::ident(class),
            ];
            let mut tb = tb;
            let schema = match (&tb.info, tb.kv) {
                (_, true) => iceberg_out::kv_schema()?,
                (Some(info), false) => iceberg_out::change_schema(info)?,
                (None, false) => continue,
            };
            let name = if tb.kv {
                table.to_string()
            } else {
                iceberg_out::ident(table)
            };
            let ident = self
                .sink
                .ensure_table(
                    &ns,
                    &name,
                    schema,
                    HashMap::from([("cdcell.original-name".to_string(), table.to_string())]),
                )
                .await?;
            // schema registry + additive evolution before the data (rev 5)
            self.sink.evolve(&ident, &mut tb.rows).await?;
            events += tb.rows.len();
            if let Some(st) = self.sink.stage(&ident, &tb.rows, props.clone()).await? {
                staged.push(st);
            }
            tables.push(ns.iter().cloned().chain([name]).collect::<Vec<_>>());
        }
        let ns = vec!["cdcell".to_string(), iceberg_out::ident(&self.cfg.script)];
        let pident = self
            .sink
            .ensure_table(
                &ns,
                "_cdcell_progress",
                iceberg_out::progress_schema()?,
                HashMap::new(),
            )
            .await?;
        let now = now_us();
        let rows: Vec<OutRow> = self
            .batch_progress
            .iter()
            .map(|(cell, (e, t, ms))| {
                OutRow::from([
                    ("_cell_id".into(), OutValue::Str(cell.clone())),
                    ("_epoch".into(), OutValue::Long(*e as i64)),
                    ("_txid".into(), OutValue::Long(*t as i64)),
                    ("captured_at".into(), OutValue::Ts(ms * 1000)),
                    ("committed_at".into(), OutValue::Ts(now)),
                    ("batch".into(), OutValue::Long(seq as i64)),
                ])
            })
            .collect();
        if let Some(st) = self.sink.stage(&pident, &rows, props.clone()).await? {
            staged.push(st);
        }
        tables.push(
            ns.iter()
                .cloned()
                .chain(["_cdcell_progress".to_string()])
                .collect(),
        );
        // 3. create-only manifest: a second writer loses here
        let manifest = serde_json::json!({"version": 1, "seq": seq, "lease_epoch": self.lease_epoch, "tables": tables, "cells": cells_doc});
        if !self
            .state
            .put_new(
                &format!("batches/{seq:020}.json"),
                serde_json::to_vec(&manifest)?,
            )
            .await?
        {
            anyhow::bail!("batch manifest {seq} already exists: another cdcell is committing");
        }
        // 4. atomic multi-table commit
        self.sink.commit_atomic(staged).await?;
        self.batch_seq = seq;
        for (cell, (e, t, _)) in std::mem::take(&mut self.batch_progress) {
            let st = self.cells.get_mut(&cell).unwrap();
            st.committed = (e, t);
            st.uncommitted_since = None;
        }
        // 5. checkpoint, then acks
        self.write_checkpoint(seq).await?;
        for m in std::mem::take(&mut self.batch_msgs) {
            self.settle(Some(m));
        }
        self.log_laggards();
        self.stats.commits += 1;
        crate::metrics::inc(&crate::metrics::M.commits);
        crate::metrics::add(
            &crate::metrics::M.commit_micros,
            commit_started.elapsed().as_micros() as u64,
        );
        info!(seq, events, cells = self.cells.len(), stats = ?self.stats, "committed batch");
        Ok(())
    }

    async fn write_checkpoint(&self, seq: u64) -> Result<()> {
        let cells: serde_json::Map<String, J> = self
            .cells
            .iter()
            .filter(|(_, s)| s.committed != (0, 0))
            .map(|(c, s)| {
                (
                    c.clone(),
                    serde_json::json!({"epoch": s.committed.0, "txid": s.committed.1}),
                )
            })
            .collect();
        let doc = serde_json::json!({"version": 1, "last_batch": seq, "lease_epoch": self.lease_epoch, "cells": cells});
        self.state
            .put("checkpoint.json", serde_json::to_vec(&doc)?)
            .await
    }

    /// Restart (design §6): checkpoint, then roll forward every manifest
    /// whose batch is visibly committed; restore cells at their committed
    /// points (cell state loads lazily). Redelivered units at or below a
    /// committed point are dropped, so nothing is emitted twice.
    pub async fn recover(&mut self) -> Result<()> {
        let mut last = 0;
        let mut cells: serde_json::Map<String, J> = serde_json::Map::new();
        if let Some(b) = self.state.get("checkpoint.json").await? {
            let doc: J = serde_json::from_slice(&b)?;
            last = doc["last_batch"].as_u64().unwrap_or(0);
            cells = doc["cells"].as_object().cloned().unwrap_or_default();
        }
        let mut highest = last;
        for key in self.state.list("batches/").await? {
            let Some(seq) = key
                .strip_prefix("batches/")
                .and_then(|n| n.strip_suffix(".json"))
                .and_then(|n| n.parse::<u64>().ok())
            else {
                continue;
            };
            highest = highest.max(seq);
            if seq <= last {
                continue;
            }
            let Some(b) = self.state.get(&key).await? else {
                continue;
            };
            let m: J = serde_json::from_slice(&b)?;
            let first = m["tables"]
                .as_array()
                .and_then(|t| t.first())
                .and_then(|t| t.as_array())
                .cloned()
                .unwrap_or_default();
            let parts: Vec<String> = first
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect();
            let committed = match parts.split_last() {
                Some((name, ns)) => {
                    self.sink
                        .has_batch(&Sink::table_ident(ns, name)?, seq)
                        .await?
                }
                None => false,
            };
            if committed {
                info!(
                    seq,
                    "rolling forward a batch committed after the last checkpoint"
                );
                for (c, v) in m["cells"].as_object().cloned().unwrap_or_default() {
                    cells.insert(c, v);
                }
                last = seq;
            } else {
                info!(
                    seq,
                    "discarding an uncommitted batch (its messages will be redelivered)"
                );
            }
        }
        self.batch_seq = highest;
        for (cell, v) in cells {
            let mut st = CellState::new(&cell);
            st.epoch = v["epoch"].as_u64();
            st.confirmed_txid = v["txid"].as_u64().unwrap_or(0);
            st.committed = (st.epoch.unwrap_or(0), st.confirmed_txid);
            st.rebuild = true;
            self.cells.insert(cell, st);
        }
        info!(
            cells = self.cells.len(),
            last_batch = last,
            next_batch = highest + 1,
            "recovered"
        );
        if last != 0 {
            self.write_checkpoint(last).await?;
        }
        Ok(())
    }

    /// Global v1 lease (design §3, §6): one committing process.
    pub async fn acquire_lease(&mut self, ttl: Duration) -> Result<()> {
        loop {
            let now = now_us() / 1000;
            let doc = |epoch: u64| {
                serde_json::to_vec(&serde_json::json!({"owner": self.owner, "epoch": epoch, "expires_ms": now + ttl.as_millis() as i64})).unwrap()
            };
            match self.state.get_versioned("lease.json").await? {
                None => {
                    if self.state.put_new("lease.json", doc(1)).await? {
                        self.lease_epoch = 1;
                        break;
                    }
                }
                Some((b, ver)) => {
                    let cur: J = serde_json::from_slice(&b)?;
                    let expired = cur["expires_ms"].as_i64().unwrap_or(0) < now;
                    let epoch = cur["epoch"].as_u64().unwrap_or(0) + 1;
                    if expired || cur["owner"] == self.owner.as_str() {
                        if self.state.put_if("lease.json", doc(epoch), ver).await? {
                            self.lease_epoch = epoch;
                            break;
                        }
                    } else {
                        warn!(holder = %cur["owner"], "another cdcell holds the lease; waiting");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
        self.lease_version = self
            .state
            .get_versioned("lease.json")
            .await?
            .map(|(_, v)| v);
        self.lease_ttl = ttl;
        info!(epoch = self.lease_epoch, owner = %self.owner, "lease acquired");
        Ok(())
    }

    pub async fn renew_lease(&mut self) -> Result<()> {
        let Some(ver) = self.lease_version.clone() else {
            return Ok(());
        };
        let body = serde_json::to_vec(
            &serde_json::json!({"owner": self.owner, "epoch": self.lease_epoch, "expires_ms": now_us() / 1000 + self.lease_ttl.as_millis() as i64}),
        )?;
        if !self.state.put_if("lease.json", body, ver).await? {
            anyhow::bail!(
                "lost the cdcell lease (epoch {}): another process took over",
                self.lease_epoch
            );
        }
        self.lease_version = self
            .state
            .get_versioned("lease.json")
            .await?
            .map(|(_, v)| v);
        Ok(())
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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}
