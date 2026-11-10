// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! Test kit: a fake celld writing real LTX objects into an in-memory bucket,
//! and a harness that drives the real [`Pipeline`] with paused time.
//!
//! The fake celld runs real SQLite per cell and captures each transaction's
//! changed pages (journal_mode=DELETE, file diff), so the decoder sees what it
//! sees in production. Objects use celld's own encoder (`celld_ltx`) and key
//! layout (`cells/<cell>/ltx/e<E>/<level>/<min>-<max>.ltx`, `own.json`).
//! What it models is listed per method; the rest (bundles, node leases,
//! paged activations, fenced owners) is added as tests need it.

#![allow(dead_code)] // each test file uses part of the kit

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cdcell::iceberg_out::{OutRow, OutValue};
use cdcell::out::MemSink;
use cdcell::pipeline::{Pipeline, Settings};
use cdcell::source::Store;
use celld_ltx::ltx::{encode_file, Header, HEADER_FLAG_NO_CHECKSUM, VERSION};
use celld_ltx::TXID;
use rusqlite::Connection;

const PAGE_SIZE: u32 = 4096;
static DIRS: AtomicU64 = AtomicU64::new(0);

struct CellSim {
    conn: Connection,
    path: PathBuf,
    epoch: u64,
    txid: u64,
    image: Vec<u8>,
}

/// A fleet node's log session: its lease and the bundle being filled.
struct Session {
    generation: String,
    log_epoch: u64,
    seq: u64,
    sealed: bool,
    pending: Vec<celld_ltx::bundle::BundleEntry>,
}

/// A celld deployment. Single-node mode: each transaction is a per-cell L0
/// object. Fleet mode (`fleet()`): transactions go into node bundles
/// (`log/<node>/<gen>/bundle/e<LE>-<seq>.ltxb`, celld's encoder) confirmed by
/// the node lease (`nodes/<node>.json`); per-cell objects appear on drains,
/// folds and snapshots.
pub struct Celld {
    pub bucket: Store,
    /// Every notification ever sent (object keys, in order).
    pub notified: Vec<String>,
    cells: BTreeMap<String, CellSim>,
    dir: PathBuf,
    ts_ms: i64,
    /// Fleet mode: the writing node's session.
    session: Option<Session>,
}

fn ltx_key(cell: &str, epoch: u64, level: u32, min: u64, max: u64) -> String {
    format!("cells/{cell}/ltx/e{epoch}/{level:04x}/{min:016x}-{max:016x}.ltx")
}

fn encode(min: u64, max: u64, commit: u32, ts: i64, pages: &[(u32, Vec<u8>)]) -> Vec<u8> {
    let header = Header {
        version: VERSION,
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size: PAGE_SIZE,
        commit,
        min_txid: TXID(min),
        max_txid: TXID(max),
        timestamp: ts,
        pre_apply_checksum: 0,
        wal_offset: 0,
        wal_size: 0,
        wal_salt1: 0,
        wal_salt2: 0,
        node_id: 0,
    };
    encode_file(&header, pages, 0).expect("encode LTX")
}

fn pages_of(image: &[u8]) -> Vec<(u32, Vec<u8>)> {
    image
        .chunks(PAGE_SIZE as usize)
        .enumerate()
        .map(|(i, p)| (i as u32 + 1, p.to_vec()))
        .collect()
}

impl Celld {
    pub fn new() -> Celld {
        let dir = std::env::temp_dir().join(format!(
            "cdcell-kit-{}-{}",
            std::process::id(),
            DIRS.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Celld {
            bucket: Store::memory(),
            notified: Vec::new(),
            cells: BTreeMap::new(),
            dir,
            ts_ms: 1_700_000_000_000,
            session: None,
        }
    }

    /// Switches to fleet mode: node `n1` opens log session generation `g1`.
    pub async fn fleet(&mut self) {
        self.session = Some(Session {
            generation: "g1".into(),
            log_epoch: 1,
            seq: 0,
            sealed: false,
            pending: Vec::new(),
        });
        self.write_lease().await;
    }

    async fn write_lease(&mut self) {
        let s = self.session.as_ref().unwrap();
        let state = if s.sealed { "sealed" } else { "open" };
        let lease = serde_json::json!({
            "node": "n1",
            "ownership_index_generation": s.generation,
            "log": {"state": state, "epoch": s.log_epoch},
        });
        self.bucket
            .put("nodes/n1.json", serde_json::to_vec(&lease).unwrap())
            .await
            .unwrap();
    }

    /// Fleet: writes the pending segments as one bundle (and notifies it).
    pub async fn flush(&mut self) {
        let s = self.session.as_mut().unwrap();
        if s.pending.is_empty() {
            return;
        }
        s.seq += 1;
        let key = format!(
            "log/n1/{}/bundle/e{}-{:016x}.ltxb",
            s.generation, s.log_epoch, s.seq
        );
        let body = celld_ltx::bundle::encode(&s.pending).expect("encode bundle");
        s.pending.clear();
        self.put(key, body).await;
    }

    /// Fleet: the node's log session is sealed (it lost its lease): its
    /// bundles can no longer be confirmed.
    pub async fn seal(&mut self) {
        self.session.as_mut().unwrap().sealed = true;
        self.write_lease().await;
    }

    /// Fleet: drains a cell's TXIDs `from..=to` into its per-cell prefix as
    /// L0 objects (what celld does before it GCs a bundle).
    pub async fn drain(&mut self, cell: &str, from: u64, to: u64) {
        let keys = self.bundle_keys().await;
        for k in keys {
            let body = self.bucket.get(&k).await.unwrap().unwrap();
            for row in cdcell_core::ltx::decode_bundle(&body).unwrap() {
                if row.cell == cell && (from..=to).contains(&row.unit.max_txid) {
                    let u = &row.unit;
                    let ltx = encode(u.min_txid, u.max_txid, u.commit, u.timestamp_ms, &u.pages);
                    self.put(
                        ltx_key(cell, row.cell_epoch, 0, u.min_txid, u.max_txid),
                        ltx,
                    )
                    .await;
                }
            }
        }
    }

    /// Fleet: like [`Self::drain`], but as one merged per-cell object
    /// `from-to` (a recovered session folded into the cell's prefix).
    pub async fn drain_merged(&mut self, cell: &str, from: u64, to: u64) {
        let mut rows = Vec::new();
        for k in self.bundle_keys().await {
            let body = self.bucket.get(&k).await.unwrap().unwrap();
            rows.extend(
                cdcell_core::ltx::decode_bundle(&body)
                    .unwrap()
                    .into_iter()
                    .filter(|r| r.cell == cell && (from..=to).contains(&r.unit.max_txid)),
            );
        }
        rows.sort_by_key(|r| r.unit.max_txid);
        let (epoch, last) = (rows[0].cell_epoch, rows.last().unwrap().unit.clone());
        let mut pages: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        for r in &rows {
            for (p, d) in &r.unit.pages {
                pages.insert(*p, d.clone());
            }
        }
        let pages: Vec<(u32, Vec<u8>)> = pages
            .into_iter()
            .filter(|(p, _)| *p <= last.commit)
            .collect();
        let ltx = encode(from, to, last.commit, last.timestamp_ms, &pages);
        self.put(ltx_key(cell, epoch, 0, from, to), ltx).await;
    }

    /// Fleet: deletes every bundle (celld GCs them once per-cell objects
    /// cover them).
    pub async fn gc_bundles(&mut self) {
        for k in self.bundle_keys().await {
            self.bucket.delete(&k).await.unwrap();
        }
    }

    async fn bundle_keys(&self) -> Vec<String> {
        self.bucket
            .list("log/")
            .await
            .unwrap()
            .into_iter()
            .filter(|k| k.ends_with(".ltxb"))
            .collect()
    }

    /// Writes one LTX for a cell: a per-cell L0 object, or in fleet mode a
    /// bundle segment.
    async fn write_l0(&mut self, cell: &str, epoch: u64, min: u64, max: u64, ltx: Vec<u8>) {
        if let Some(s) = self.session.as_mut() {
            s.pending.push(celld_ltx::bundle::BundleEntry {
                cell: cell.to_string(),
                cell_epoch: epoch,
                txid: max,
                bytes: ltx,
            });
        } else {
            self.put(ltx_key(cell, epoch, 0, min, max), ltx).await;
        }
    }

    async fn put(&mut self, key: String, body: Vec<u8>) {
        self.bucket.put(&key, body).await.unwrap();
        self.notified.push(key);
    }

    fn tick(&mut self) -> i64 {
        self.ts_ms += 1;
        self.ts_ms
    }

    /// A new Durable Object of class `Shop` named `name`, created by `ddl`,
    /// activated in epoch 1 (TXID 1 = its first full image).
    pub async fn create(&mut self, name: &str, ddl: &str) -> String {
        let cell = format!("Shop:{:0>64}", hex(name));
        let path = self.dir.join(format!("{}.db", hex(name)));
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA page_size={PAGE_SIZE}; PRAGMA journal_mode=DELETE; {ddl}"
        ))
        .unwrap();
        let image = std::fs::read(&path).unwrap();
        self.cells.insert(
            cell.clone(),
            CellSim {
                conn,
                path,
                epoch: 0,
                txid: 0,
                image,
            },
        );
        self.activate(&cell, 1).await;
        cell
    }

    /// An activation (restore from a full image): own.json names the new
    /// epoch; TXIDs restart at 1 with a full-image L0 (the TXID-1 opener).
    pub async fn activate(&mut self, cell: &str, epoch: u64) {
        let ts = self.tick();
        let c = self.cells.get_mut(cell).unwrap();
        c.epoch = epoch;
        c.txid = 1;
        let commit = (c.image.len() / PAGE_SIZE as usize) as u32;
        let body = encode(1, 1, commit, ts, &pages_of(&c.image));
        let own = serde_json::to_vec(&serde_json::json!({"node": "n1", "epoch": epoch})).unwrap();
        self.bucket
            .put(&format!("cells/{cell}/own.json"), own)
            .await
            .unwrap();
        self.write_l0(cell, epoch, 1, 1, body).await;
    }

    /// A paged activation (celld D2, a large chain restored lazily): the new
    /// epoch opens with a zero-page marker at `cut + 1`, and TXIDs continue
    /// from there; the base stays in the lower epochs (the restore chain).
    pub async fn activate_paged(&mut self, cell: &str, epoch: u64) {
        let ts = self.tick();
        let c = self.cells.get_mut(cell).unwrap();
        let marker = c.txid + 1;
        c.epoch = epoch;
        c.txid = marker;
        let commit = (c.image.len() / PAGE_SIZE as usize) as u32;
        let body = encode(marker, marker, commit, ts, &[]);
        let own = serde_json::to_vec(&serde_json::json!({"node": "n1", "epoch": epoch})).unwrap();
        self.bucket
            .put(&format!("cells/{cell}/own.json"), own)
            .await
            .unwrap();
        self.write_l0(cell, epoch, marker, marker, body).await;
    }

    /// One transaction: its changed pages become the next TXID's L0.
    pub async fn exec(&mut self, cell: &str, sql: &str) {
        let ts = self.tick();
        let c = self.cells.get_mut(cell).unwrap();
        c.conn
            .execute_batch(&format!("BEGIN; {sql}; COMMIT;"))
            .unwrap();
        let now = std::fs::read(&c.path).unwrap();
        let ps = PAGE_SIZE as usize;
        let changed: Vec<(u32, Vec<u8>)> = now
            .chunks(ps)
            .enumerate()
            .filter(|(i, p)| c.image.get(i * ps..(i + 1) * ps) != Some(*p))
            .map(|(i, p)| (i as u32 + 1, p.to_vec()))
            .collect();
        c.image = now;
        if changed.is_empty() {
            return;
        }
        c.txid += 1;
        let (epoch, txid) = (c.epoch, c.txid);
        let commit = (c.image.len() / ps) as u32;
        let body = encode(txid, txid, commit, ts, &changed);
        self.write_l0(cell, epoch, txid, txid, body).await;
    }

    /// An L9 snapshot (a clean close/handoff): the full image, TXID 1..now.
    pub async fn l9(&mut self, cell: &str) {
        let ts = self.tick();
        let c = &self.cells[cell];
        let (epoch, txid, commit) = (c.epoch, c.txid, (c.image.len() / PAGE_SIZE as usize) as u32);
        let body = encode(1, txid, commit, ts, &pages_of(&c.image));
        self.put(ltx_key(cell, epoch, 9, 1, txid), body).await;
    }

    /// An L1 fold from TXID 1 (the full image up to now); the folded L0s
    /// are deleted.
    pub async fn fold_l1(&mut self, cell: &str) {
        let ts = self.tick();
        let c = &self.cells[cell];
        let (epoch, txid, commit) = (c.epoch, c.txid, (c.image.len() / PAGE_SIZE as usize) as u32);
        let body = encode(1, txid, commit, ts, &pages_of(&c.image));
        self.put(ltx_key(cell, epoch, 1, 1, txid), body).await;
        for k in self
            .bucket
            .list(&format!("cells/{cell}/ltx/e{epoch}/0000/"))
            .await
            .unwrap()
        {
            self.bucket.delete(&k).await.unwrap();
        }
    }

    /// celld's epoch GC (opt-in): delete one epoch prefix of a cell.
    pub async fn gc_epoch(&mut self, cell: &str, epoch: u64) {
        for k in self
            .bucket
            .list(&format!("cells/{cell}/ltx/e{epoch}/"))
            .await
            .unwrap()
        {
            self.bucket.delete(&k).await.unwrap();
        }
    }

    /// The oracle: `SELECT <cols> FROM <table>` as key → values (first
    /// column is the key, as cdcell's `__key` JSON array).
    pub fn rows(&self, cell: &str, sql: &str) -> BTreeMap<String, Vec<String>> {
        let c = &self.cells[cell];
        let mut stmt = c.conn.prepare(sql).unwrap();
        let n = stmt.column_count();
        let mut out = BTreeMap::new();
        let mut rows = stmt.query([]).unwrap();
        while let Some(r) = rows.next().unwrap() {
            let vals: Vec<String> = (0..n)
                .map(|i| match r.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => "null".into(),
                    rusqlite::types::ValueRef::Integer(v) => v.to_string(),
                    rusqlite::types::ValueRef::Real(v) => v.to_string(),
                    rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
                    rusqlite::types::ValueRef::Blob(_) => "blob".into(),
                })
                .collect();
            out.insert(format!("[{}]", vals[0]), vals);
        }
        out
    }
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

/// Drives the real pipeline against a [`Celld`], with a [`MemSink`] and an
/// in-memory state bucket. Run tests with `#[tokio::test(start_paused = true)]`.
pub struct Harness {
    pub celld: Celld,
    pub state: Store,
    pub sink: Arc<MemSink>,
    pub pipe: Pipeline,
    /// Index into `celld.notified` delivered so far.
    delivered: usize,
}

pub const GAP_TIMEOUT: Duration = Duration::from_secs(15);

async fn start(celld: &Celld, state: &Store, sink: &Arc<MemSink>) -> Pipeline {
    let mut pipe = Pipeline::new(
        celld.bucket.clone(),
        state.clone(),
        Box::new(sink.clone()),
        Settings {
            script: "shop".into(),
            gap_timeout: GAP_TIMEOUT,
        },
    );
    pipe.acquire_lease(Duration::from_secs(30)).await.unwrap();
    pipe.recover().await.unwrap();
    pipe
}

impl Harness {
    pub async fn new() -> Harness {
        cdcell::pipeline::use_test_clock();
        // logs show with `cargo test -- --nocapture` (RUST_LOG-style filter)
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(std::env::var("CDCELL_TEST_LOG").unwrap_or_else(|_| "warn".into()))
            .try_init();
        let celld = Celld::new();
        let state = Store::memory();
        let sink = Arc::new(MemSink::new());
        let pipe = start(&celld, &state, &sink).await;
        Harness {
            celld,
            state,
            sink,
            pipe,
            delivered: 0,
        }
    }

    /// Delivers every notification not yet delivered (intake: GET, decode).
    pub async fn deliver(&mut self) {
        let keys: Vec<String> = self.celld.notified[self.delivered..].to_vec();
        self.delivered = self.celld.notified.len();
        for k in keys {
            cdcell::intake::ingest(&mut self.pipe, &self.celld.bucket, &k)
                .await
                .unwrap();
        }
    }

    /// Delivers the undelivered notifications whose key contains `needle`,
    /// leaving the others queued (controls arrival order).
    pub async fn deliver_matching(&mut self, needle: &str) {
        let keys: Vec<String> = self.celld.notified[self.delivered..]
            .iter()
            .filter(|k| k.contains(needle))
            .cloned()
            .collect();
        let rest: Vec<String> = self.celld.notified[self.delivered..]
            .iter()
            .filter(|k| !k.contains(needle))
            .cloned()
            .collect();
        // keep the not-yet-delivered ones at the end, in order
        let done = self.delivered;
        self.celld.notified.truncate(done);
        self.celld.notified.extend(keys.iter().cloned());
        self.delivered = self.celld.notified.len();
        self.celld.notified.extend(rest);
        for k in keys {
            cdcell::intake::ingest(&mut self.pipe, &self.celld.bucket, &k)
                .await
                .unwrap();
        }
    }

    /// Runs confirmation/apply rounds for `secs` of simulated time (1 s
    /// apart, past the gap timeout), then commits.
    pub async fn run(&mut self, secs: u64) {
        for _ in 0..secs {
            self.pipe.round().await.unwrap();
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        self.pipe.commit().await.unwrap();
    }

    /// cdcell stops; notifications sent while it is down stay queued.
    pub fn stop(&mut self) {}

    /// cdcell restarts: the old process is gone (its lease expires), the new
    /// one acquires the lease, recovers, and gets every unacked notification
    /// again (redelivery; duplicates are allowed).
    pub async fn restart(&mut self) {
        tokio::time::advance(Duration::from_secs(31)).await;
        self.pipe = start(&self.celld, &self.state, &self.sink).await;
        self.delivered = 0;
    }

    /// Type 1 of a change-log table: latest row per (cell, key) by
    /// (epoch, txid), deletes removed; values of `cols` as strings.
    pub fn type1(&self, table: &str, cell: &str, cols: &[&str]) -> BTreeMap<String, Vec<String>> {
        let mut latest: BTreeMap<String, (i64, i64, OutRow)> = BTreeMap::new();
        for r in self.sink.rows(table) {
            if s(&r, "__cell_id") != cell {
                continue;
            }
            let (e, t) = (l(&r, "__epoch"), l(&r, "__txid"));
            let k = s(&r, "__key");
            if latest
                .get(&k)
                .is_none_or(|(le, lt, _)| (e, t) >= (*le, *lt))
            {
                latest.insert(k, (e, t, r));
            }
        }
        latest
            .into_iter()
            .filter(|(_, (_, _, r))| s(r, "__op") != "d")
            .map(|(k, (_, _, r))| (k, cols.iter().map(|c| v(&r, c)).collect()))
            .collect()
    }
}

fn s(r: &OutRow, c: &str) -> String {
    match r.get(c) {
        Some(OutValue::Str(v)) => v.clone(),
        _ => String::new(),
    }
}
fn l(r: &OutRow, c: &str) -> i64 {
    match r.get(c) {
        Some(OutValue::Long(v)) => *v,
        _ => 0,
    }
}
fn v(r: &OutRow, c: &str) -> String {
    match r.get(c) {
        None | Some(OutValue::Null) => "null".into(),
        Some(OutValue::Long(x)) => x.to_string(),
        Some(OutValue::Double(x)) => x.to_string(),
        Some(OutValue::Str(x)) => x.clone(),
        Some(other) => format!("{other:?}"),
    }
}
