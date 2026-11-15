//! Table schemas: `sqlite_schema` rows (read with the b-tree decoder) plus
//! column metadata from SQLite itself (`PRAGMA table_xinfo`) over a temporary
//! copy of the image. Only re-read when the schema cookie changes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::btree::{self, Pages, View};
use crate::value::Value;
use crate::{Error, Result};

/// Declared-type affinity (https://www.sqlite.org/datatype3.html §3.1), with
/// `Untyped` separated from BLOB (design §8.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Affinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
    Untyped,
}

impl Affinity {
    pub fn from_decl(decl: &str) -> Affinity {
        let d = decl.to_ascii_uppercase();
        if d.trim().is_empty() {
            Affinity::Untyped
        } else if d.contains("INT") {
            Affinity::Integer
        } else if d.contains("CHAR") || d.contains("CLOB") || d.contains("TEXT") {
            Affinity::Text
        } else if d.contains("BLOB") {
            Affinity::Blob
        } else if d.contains("REAL") || d.contains("FLOA") || d.contains("DOUB") {
            Affinity::Real
        } else {
            Affinity::Numeric
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Generated {
    Stored,
    Virtual,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub decl_type: String,
    pub affinity: Affinity,
    pub not_null: bool,
    /// Value used for records written before an `ADD COLUMN`.
    pub default: Value,
    pub generated: Option<Generated>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableInfo {
    pub name: String,
    pub root: u32,
    pub sql: String,
    pub without_rowid: bool,
    /// All columns in declaration order.
    pub columns: Vec<Column>,
    /// Record field i holds column `record_order[i]`.
    pub record_order: Vec<usize>,
    /// The INTEGER PRIMARY KEY column, stored as NULL in records (the rowid).
    pub rowid_alias: Option<usize>,
    /// Primary key columns in key order (empty: no declared PK).
    pub pk: Vec<usize>,
}

impl TableInfo {
    /// Number of leading record fields that form the b-tree key of a
    /// WITHOUT ROWID table.
    pub fn index_key_len(&self) -> usize {
        if self.without_rowid {
            self.pk.len()
        } else {
            0
        }
    }

    /// Decodes a record into values per column (declaration order). Missing
    /// trailing fields take the column default; the rowid alias takes the rowid;
    /// VIRTUAL generated columns are not stored and stay NULL.
    pub fn row(&self, rowid: Option<i64>, record: &[Value]) -> Vec<Value> {
        let mut out: Vec<Value> = self.columns.iter().map(|c| c.default.clone()).collect();
        for (c, col) in self.columns.iter().enumerate() {
            if col.generated == Some(Generated::Virtual) {
                out[c] = Value::Null;
            }
        }
        for (i, &c) in self.record_order.iter().enumerate() {
            if let Some(v) = record.get(i) {
                out[c] = v.clone();
            }
        }
        if let (Some(a), Some(r)) = (self.rowid_alias, rowid) {
            out[a] = Value::Integer(r);
        }
        out
    }
}

/// One row of `sqlite_schema`.
#[derive(Clone, Debug)]
pub struct SchemaRow {
    pub kind: String,
    pub name: String,
    pub root: u32,
    pub sql: String,
}

pub fn schema_cookie(
    pages: &Pages,
    overlay: Option<&std::collections::HashMap<u32, Vec<u8>>>,
) -> Option<u32> {
    let p1 = overlay
        .and_then(|o| o.get(&1))
        .or_else(|| pages.pages.get(&1))?;
    Some(crate::format::be_u32(&p1[40..44]))
}

/// Reads `sqlite_schema` (the table b-tree rooted at page 1).
pub fn read_schema_rows(
    view: &View<'_>,
    enc: crate::format::TextEncoding,
) -> Result<Vec<SchemaRow>> {
    let s = btree::walk(view, 1)?;
    let mut rows = Vec::new();
    for p in s.entry_pages() {
        for e in btree::entries(view, p, enc)? {
            let text = |i: usize| match e.record.get(i) {
                Some(Value::Text(t)) => t.clone(),
                _ => String::new(),
            };
            let root = match e.record.get(3) {
                Some(Value::Integer(r)) => *r as u32,
                _ => 0,
            };
            rows.push(SchemaRow {
                kind: text(0),
                name: text(1),
                root,
                sql: text(4),
            });
        }
    }
    Ok(rows)
}

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Full table metadata for every ordinary table with a b-tree (virtual tables
/// have root page 0 and are skipped).
pub fn load_tables(image: &[u8], rows: &[SchemaRow]) -> Result<BTreeMap<String, TableInfo>> {
    let tables: Vec<&SchemaRow> = rows
        .iter()
        .filter(|r| r.kind == "table" && r.root != 0)
        .collect();
    if tables.is_empty() {
        return Ok(BTreeMap::new());
    }
    // A WAL-mode image (header bytes 18/19 == 2) would make SQLite look for a
    // WAL; mark the private copy as rollback-journal.
    let mut copy = image.to_vec();
    copy[18] = 1;
    copy[19] = 1;
    let path = std::env::temp_dir().join(format!(
        "cdcell-schema-{}-{}.sqlite",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, &copy).map_err(|e| Error::Schema(e.to_string()))?;
    let result = load_with_sqlite(&path, &tables);
    let _ = std::fs::remove_file(&path);
    result
}

fn load_with_sqlite(
    path: &std::path::Path,
    tables: &[&SchemaRow],
) -> Result<BTreeMap<String, TableInfo>> {
    use rusqlite::{Connection, OpenFlags};
    let err = |e: rusqlite::Error| Error::Schema(e.to_string());
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(err)?;
    let mut out = BTreeMap::new();
    for t in tables {
        let quoted = t.name.replace('"', "\"\"");
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_xinfo(\"{quoted}\")"))
            .map_err(err)?;
        // cid, name, type, notnull, dflt_value, pk, hidden
        let raw: Vec<(String, String, bool, Option<String>, i64, i64)> = stmt
            .query_map([], |r| {
                Ok((
                    r.get(1)?,
                    r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                ))
            })
            .map_err(err)?
            .collect::<std::result::Result<_, _>>()
            .map_err(err)?;
        let mut columns = Vec::new();
        let mut pk_pos = Vec::new();
        for (name, decl, not_null, dflt, pk, hidden) in raw {
            if hidden == 1 {
                continue; // hidden column of a virtual table
            }
            let default = match dflt {
                Some(expr) => conn
                    .query_row(&format!("SELECT {expr}"), [], |r| {
                        r.get::<_, rusqlite::types::Value>(0)
                    })
                    .map(from_sqlite)
                    .unwrap_or(Value::Null),
                None => Value::Null,
            };
            let generated = match hidden {
                2 => Some(Generated::Virtual),
                3 => Some(Generated::Stored),
                _ => None,
            };
            if pk > 0 {
                pk_pos.push((pk, columns.len()));
            }
            columns.push(Column {
                affinity: Affinity::from_decl(&decl),
                decl_type: decl,
                name,
                not_null,
                default,
                generated,
            });
        }
        pk_pos.sort();
        let pk: Vec<usize> = pk_pos.into_iter().map(|(_, c)| c).collect();
        let without_rowid = is_without_rowid(&t.sql);
        let rowid_alias = (!without_rowid
            && pk.len() == 1
            && columns[pk[0]].decl_type.eq_ignore_ascii_case("INTEGER")
            && !has_pk_desc(&t.sql))
        .then(|| pk[0]);
        let stored: Vec<usize> = (0..columns.len())
            .filter(|&c| columns[c].generated != Some(Generated::Virtual))
            .collect();
        let record_order = if without_rowid {
            let mut order = pk.clone();
            order.extend(stored.iter().copied().filter(|c| !pk.contains(c)));
            order
        } else {
            stored
        };
        out.insert(
            t.name.clone(),
            TableInfo {
                name: t.name.clone(),
                root: t.root,
                sql: t.sql.clone(),
                without_rowid,
                columns,
                record_order,
                rowid_alias,
                pk,
            },
        );
    }
    Ok(out)
}

fn from_sqlite(v: rusqlite::types::Value) -> Value {
    use rusqlite::types::Value as V;
    match v {
        V::Null => Value::Null,
        V::Integer(i) => Value::Integer(i),
        V::Real(f) => Value::Real(f),
        V::Text(s) => Value::Text(s),
        V::Blob(b) => Value::Blob(b),
    }
}

fn normalized(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase()
}

fn is_without_rowid(sql: &str) -> bool {
    let n = normalized(sql);
    let n = n.trim_end_matches(';').trim_end();
    n.ends_with("WITHOUT ROWID") || n.contains(") WITHOUT ROWID")
}

/// `INTEGER PRIMARY KEY DESC` is not a rowid alias (a SQLite quirk).
fn has_pk_desc(sql: &str) -> bool {
    normalized(sql).contains("INTEGER PRIMARY KEY DESC")
}

/// Tables cdcell never emits (design §6).
pub fn is_internal(name: &str) -> bool {
    name.starts_with("sqlite_")
        || name.starts_with("_litestream_")
        || matches!(
            name,
            "_cf_WAKE" | "_cf_FACETS" | "_cf_METADATA" | "_cf_ALARM"
        )
}
