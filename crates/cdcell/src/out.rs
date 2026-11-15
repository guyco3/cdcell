// Copyright 2026 Guy Cohen. Apache-2.0 license.

//! The change-log output seam: what the pipeline needs from an Iceberg
//! catalog (design §6), as a trait.
//!
//! [`crate::iceberg_out::Sink`] is the real implementation (REST catalog,
//! Parquet on object storage, one atomic multi-table commit). [`MemSink`]
//! keeps tables in memory with the same commit semantics, so pipeline
//! scenarios (crashes, resyncs, epoch changes) run as fast deterministic
//! tests and can read the change log back.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use iceberg::spec::Schema;
use iceberg::{NamespaceIdent, TableIdent};

use crate::iceberg_out::{OutRow, OutValue, Sink, Staged, StagedBody};

#[async_trait]
pub trait Out: Send + Sync {
    /// Creates the table if missing; returns its identifier.
    async fn ensure_table(
        &self,
        ns: &[String],
        name: &str,
        schema: Schema,
        props: HashMap<String, String>,
    ) -> Result<TableIdent>;
    /// Schema registry and additive evolution before a data commit; maps
    /// each row's fingerprint to `__schema_version`.
    async fn evolve(&self, ident: &TableIdent, rows: &mut [OutRow]) -> Result<()>;
    /// Prepares an append (snapshot properties included) without committing.
    async fn stage(
        &self,
        ident: &TableIdent,
        rows: &[OutRow],
        snapshot_props: HashMap<String, String>,
    ) -> Result<Option<Staged>>;
    /// Commits every staged change atomically: all tables or none.
    async fn commit_atomic(&self, staged: Vec<Staged>) -> Result<()>;
    /// Whether a snapshot of the table carries `cdcell.batch = seq`.
    async fn has_batch(&self, ident: &TableIdent, seq: u64) -> Result<bool>;
    /// Keys of `cell` whose latest row is not a delete.
    async fn live_keys(&self, ns: &[String], name: &str, cell: &str) -> Result<BTreeSet<String>>;
}

#[async_trait]
impl Out for Sink {
    async fn ensure_table(
        &self,
        ns: &[String],
        name: &str,
        schema: Schema,
        props: HashMap<String, String>,
    ) -> Result<TableIdent> {
        Sink::ensure_table(self, ns, name, schema, props).await
    }
    async fn evolve(&self, ident: &TableIdent, rows: &mut [OutRow]) -> Result<()> {
        Sink::evolve(self, ident, rows).await
    }
    async fn stage(
        &self,
        ident: &TableIdent,
        rows: &[OutRow],
        snapshot_props: HashMap<String, String>,
    ) -> Result<Option<Staged>> {
        Sink::stage(self, ident, rows, snapshot_props).await
    }
    async fn commit_atomic(&self, staged: Vec<Staged>) -> Result<()> {
        Sink::commit_atomic(self, staged).await
    }
    async fn has_batch(&self, ident: &TableIdent, seq: u64) -> Result<bool> {
        Sink::has_batch(self, ident, seq).await
    }
    async fn live_keys(&self, ns: &[String], name: &str, cell: &str) -> Result<BTreeSet<String>> {
        Sink::live_keys(self, ns, name, cell).await
    }
}

#[async_trait]
impl<T: Out + ?Sized> Out for std::sync::Arc<T> {
    async fn ensure_table(
        &self,
        ns: &[String],
        name: &str,
        schema: Schema,
        props: HashMap<String, String>,
    ) -> Result<TableIdent> {
        (**self).ensure_table(ns, name, schema, props).await
    }
    async fn evolve(&self, ident: &TableIdent, rows: &mut [OutRow]) -> Result<()> {
        (**self).evolve(ident, rows).await
    }
    async fn stage(
        &self,
        ident: &TableIdent,
        rows: &[OutRow],
        snapshot_props: HashMap<String, String>,
    ) -> Result<Option<Staged>> {
        (**self).stage(ident, rows, snapshot_props).await
    }
    async fn commit_atomic(&self, staged: Vec<Staged>) -> Result<()> {
        (**self).commit_atomic(staged).await
    }
    async fn has_batch(&self, ident: &TableIdent, seq: u64) -> Result<bool> {
        (**self).has_batch(ident, seq).await
    }
    async fn live_keys(&self, ns: &[String], name: &str, cell: &str) -> Result<BTreeSet<String>> {
        (**self).live_keys(ns, name, cell).await
    }
}

/// How the next [`MemSink::commit_atomic`] fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitFault {
    /// Rejected: nothing is applied.
    Definite,
    /// Applied, but the caller sees an error (a lost response).
    Ambiguous,
}

#[derive(Default)]
struct MemTable {
    /// `cdcell.schemas`: fingerprint → `__schema_version`.
    registry: BTreeMap<String, i64>,
    /// Committed snapshots: (snapshot properties, rows).
    snapshots: Vec<(HashMap<String, String>, Vec<OutRow>)>,
}

#[derive(Default)]
struct MemState {
    tables: BTreeMap<String, MemTable>,
    fault: Option<CommitFault>,
    commits: u64,
}

/// An in-memory change-log catalog with atomic multi-table commits.
#[derive(Default)]
pub struct MemSink {
    state: Mutex<MemState>,
}

fn table_key(ident: &TableIdent) -> String {
    format!(
        "{}.{}",
        ident.namespace().clone().inner().join("."),
        ident.name()
    )
}

impl MemSink {
    pub fn new() -> MemSink {
        MemSink::default()
    }

    /// Makes the next commit fail as `fault`.
    pub fn fail_next_commit(&self, fault: CommitFault) {
        self.state.lock().unwrap().fault = Some(fault);
    }

    /// Successful (applied) commits so far.
    pub fn commits(&self) -> u64 {
        self.state.lock().unwrap().commits
    }

    /// Committed tables, as `ns.….name`.
    pub fn tables(&self) -> Vec<String> {
        self.state.lock().unwrap().tables.keys().cloned().collect()
    }

    /// Every committed row of the table whose key ends with `.{name}`
    /// (e.g. `"orders"`), in commit order.
    pub fn rows(&self, name: &str) -> Vec<OutRow> {
        let st = self.state.lock().unwrap();
        let suffix = format!(".{name}");
        st.tables
            .iter()
            .filter(|(k, _)| k.ends_with(&suffix))
            .flat_map(|(_, t)| {
                t.snapshots
                    .iter()
                    .flat_map(|(_, rows)| rows.iter().cloned())
            })
            .collect()
    }
}

#[async_trait]
impl Out for MemSink {
    async fn ensure_table(
        &self,
        ns: &[String],
        name: &str,
        _schema: Schema,
        _props: HashMap<String, String>,
    ) -> Result<TableIdent> {
        let ident = TableIdent::new(NamespaceIdent::from_vec(ns.to_vec())?, name.to_string());
        self.state
            .lock()
            .unwrap()
            .tables
            .entry(table_key(&ident))
            .or_default();
        Ok(ident)
    }

    async fn evolve(&self, ident: &TableIdent, rows: &mut [OutRow]) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        let t = st.tables.entry(table_key(ident)).or_default();
        for row in rows.iter_mut() {
            if let Some(OutValue::Str(fp)) = row.remove(crate::emit::FINGERPRINT) {
                let next = t.registry.values().max().copied().unwrap_or(0) + 1;
                let v = *t.registry.entry(fp).or_insert(next);
                row.insert("__schema_version".into(), OutValue::Long(v));
            }
        }
        Ok(())
    }

    async fn stage(
        &self,
        ident: &TableIdent,
        rows: &[OutRow],
        snapshot_props: HashMap<String, String>,
    ) -> Result<Option<Staged>> {
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(Staged {
            ident: ident.clone(),
            body: StagedBody::Mem {
                rows: rows.to_vec(),
                props: snapshot_props,
            },
        }))
    }

    async fn commit_atomic(&self, staged: Vec<Staged>) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        let fault = st.fault.take();
        if fault == Some(CommitFault::Definite) {
            return Err(anyhow!("injected: commit rejected"));
        }
        let mut changes = Vec::new();
        for s in staged {
            match s.body {
                StagedBody::Mem { rows, props } => changes.push((table_key(&s.ident), props, rows)),
                StagedBody::Rest { .. } => {
                    return Err(anyhow!("REST staged change sent to MemSink"))
                }
            }
        }
        for (key, props, rows) in changes {
            st.tables
                .entry(key)
                .or_default()
                .snapshots
                .push((props, rows));
        }
        st.commits += 1;
        if fault == Some(CommitFault::Ambiguous) {
            return Err(anyhow!("injected: commit applied, response lost"));
        }
        Ok(())
    }

    async fn has_batch(&self, ident: &TableIdent, seq: u64) -> Result<bool> {
        let st = self.state.lock().unwrap();
        let want = seq.to_string();
        Ok(st.tables.get(&table_key(ident)).is_some_and(|t| {
            t.snapshots
                .iter()
                .any(|(p, _)| p.get("cdcell.batch") == Some(&want))
        }))
    }

    async fn live_keys(&self, ns: &[String], name: &str, cell: &str) -> Result<BTreeSet<String>> {
        let ident = TableIdent::new(NamespaceIdent::from_vec(ns.to_vec())?, name.to_string());
        let st = self.state.lock().unwrap();
        let Some(t) = st.tables.get(&table_key(&ident)) else {
            return Ok(BTreeSet::new());
        };
        let s = |r: &OutRow, c: &str| match r.get(c) {
            Some(OutValue::Str(v)) => v.clone(),
            _ => String::new(),
        };
        let l = |r: &OutRow, c: &str| match r.get(c) {
            Some(OutValue::Long(v)) => *v,
            _ => 0,
        };
        let rows = t
            .snapshots
            .iter()
            .flat_map(|(_, rows)| rows.iter())
            .filter(|r| s(r, "__cell_id") == cell)
            .map(|r| (s(r, "__key"), s(r, "__op"), l(r, "__epoch"), l(r, "__txid")));
        Ok(crate::resync::live_keys(rows))
    }
}
