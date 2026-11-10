//! Page diffs → row changes (design §6).
//!
//! The decoder holds a cell's pages at its confirmed point (the shadow) and,
//! for each table, the set of b-tree pages and an overflow-owner map. Applying
//! one unit (the changed pages of an LTX file, plus the new database size)
//! decodes only the entries on changed entry pages, before and after, and
//! matches them by key. A row that moves between pages changes both pages, so
//! the diff is exact regardless of how SQLite rebalanced the tree.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::btree::{self, Pages, Structure, View};
use crate::schema::{self, TableInfo};
use crate::value::{key_json, Value};
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    /// Debezium op code.
    pub fn code(self) -> &'static str {
        match self {
            Op::Insert => "c",
            Op::Update => "u",
            Op::Delete => "d",
        }
    }
}

/// A row image: (column name, value) in declaration order.
pub type Row = Vec<(String, Value)>;

#[derive(Clone, Debug, PartialEq)]
pub struct RowChange {
    pub table: String,
    pub op: Op,
    /// Declared primary key values; `[rowid]` for tables keyed by rowid.
    pub key: Vec<Value>,
    pub before: Option<Row>,
    pub after: Option<Row>,
}

impl RowChange {
    pub fn key_json(&self) -> String {
        key_json(&self.key)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SchemaChange {
    CreateTable { table: String, sql: String },
    DropTable { table: String, sql: String },
    AlterTable {
        table: String,
        old_sql: String,
        new_sql: String,
        /// A column rename (design §8.6): same column count, same declared
        /// types by position, exactly one name differs. Otherwise `None` and
        /// the change is DROP + ADD as seen by name.
        renamed: Option<(String, String)>,
    },
}

fn detect_rename(old: &TableInfo, new: &TableInfo) -> Option<(String, String)> {
    if old.columns.len() != new.columns.len() {
        return None;
    }
    let mut diff = old.columns.iter().zip(&new.columns).filter(|(a, b)| a.name != b.name || a.decl_type != b.decl_type);
    match (diff.next(), diff.next()) {
        (Some((a, b)), None) if a.decl_type == b.decl_type => Some((a.name.clone(), b.name.clone())),
        _ => None,
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Changes {
    pub rows: Vec<RowChange>,
    pub schema: Vec<SchemaChange>,
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub units: u64,
    pub pages_in: u64,
    pub pages_unchanged: u64,
    pub pages_decoded: u64,
    pub rewalks: u64,
    pub schema_reads: u64,
}

#[derive(Clone, Debug)]
struct TableState {
    info: TableInfo,
    structure: Structure,
    /// overflow page → entry page holding the cell that owns it.
    overflow_owner: HashMap<u32, u32>,
}

/// Incremental decoder for one cell.
#[derive(Clone, Debug, Default)]
pub struct Decoder {
    pages: Pages,
    tables: BTreeMap<String, TableState>,
    cookie: Option<u32>,
    pub stats: Stats,
}

impl Decoder {
    pub fn new() -> Decoder {
        Decoder::default()
    }

    /// Starts from a full database image (bootstrap, shadow checkpoint).
    pub fn from_image(image: &[u8]) -> Result<Decoder> {
        let pages = Pages::from_image(image)?;
        let mut d = Decoder { pages, ..Default::default() };
        let view = View::before(&d.pages);
        let enc = d.pages.encoding();
        let rows = schema::read_schema_rows(&view, enc)?;
        let infos = schema::load_tables(image, &rows)?;
        for (name, info) in infos {
            let structure = btree::walk(&view, info.root)?;
            let mut overflow_owner = HashMap::new();
            for p in structure.entry_pages() {
                for e in btree::entries(&view, p, enc)? {
                    for o in e.overflow {
                        overflow_owner.insert(o, p);
                    }
                }
            }
            d.tables.insert(name, TableState { info, structure, overflow_owner });
        }
        d.cookie = schema::schema_cookie(&d.pages, None);
        Ok(d)
    }

    pub fn pages(&self) -> &Pages {
        &self.pages
    }

    pub fn table_info(&self, table: &str) -> Option<&TableInfo> {
        self.tables.get(table).map(|t| &t.info)
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableInfo> {
        self.tables.values().map(|t| &t.info)
    }

    /// Every row of a table at the current point, by key JSON (bootstrap `r`
    /// events, tests).
    pub fn rows(&self, table: &str) -> Result<BTreeMap<String, (Vec<Value>, Row)>> {
        let t = self.tables.get(table).ok_or_else(|| Error::Schema(format!("no table {table}")))?;
        let view = View::before(&self.pages);
        let enc = self.pages.encoding();
        let mut out = BTreeMap::new();
        for p in t.structure.entry_pages() {
            for e in btree::entries(&view, p, enc)? {
                let (key, row) = keyed_row(&t.info, &e);
                out.insert(key_json(&key), (key, row));
            }
        }
        Ok(out)
    }

    /// Applies one unit: the changed pages and the new database size.
    pub fn apply(&mut self, changed: Vec<(u32, Vec<u8>)>, commit: u32) -> Result<Changes> {
        self.stats.units += 1;
        self.stats.pages_in += changed.len() as u64;
        if self.pages.page_size == 0 {
            let p1 = changed.iter().find(|(p, _)| *p == 1).ok_or(Error::MissingPage(1))?;
            self.pages.page_size = btree::page_size_from_header(&p1.1);
            self.pages.usable = self.pages.page_size - usize::from(p1.1[20]);
        }
        // Drop pages beyond the new size and pages identical to the shadow
        // (full-image units rewrite every page).
        let mut overlay: HashMap<u32, Vec<u8>> = HashMap::new();
        for (pgno, data) in changed {
            if pgno == 0 || pgno > commit {
                continue;
            }
            if data.len() != self.pages.page_size {
                return Err(Error::Malformed("page of the wrong size"));
            }
            if self.pages.pages.get(&pgno).is_some_and(|old| *old == data) {
                self.stats.pages_unchanged += 1;
                continue;
            }
            overlay.insert(pgno, data);
        }
        let old_commit = self.pages.commit;
        let mut dirty: BTreeSet<u32> = overlay.keys().copied().collect();
        dirty.extend((commit + 1)..=old_commit);
        if dirty.is_empty() && commit == old_commit {
            return Ok(Changes::default());
        }

        let mut out = Changes::default();
        let new_cookie = schema::schema_cookie(&self.pages, Some(&overlay));
        let new_infos: BTreeMap<String, TableInfo> = if new_cookie != self.cookie || (self.tables.is_empty() && overlay.contains_key(&1)) {
            self.stats.schema_reads += 1;
            let after = View::after(&self.pages, &overlay, commit);
            let rows = schema::read_schema_rows(&after, self.encoding_after(&overlay))?;
            let image = image_with(&self.pages, &overlay, commit);
            schema::load_tables(&image, &rows)?
        } else {
            self.tables.iter().map(|(n, t)| (n.clone(), t.info.clone())).collect()
        };

        let before = View::before(&self.pages);
        let after = View::after(&self.pages, &overlay, commit);
        let enc_before = self.pages.encoding();
        let enc_after = self.encoding_after(&overlay);
        let mut new_tables = BTreeMap::new();
        let names: BTreeSet<String> = self.tables.keys().chain(new_infos.keys()).cloned().collect();
        for name in names {
            let old = self.tables.get(&name);
            let Some(info) = new_infos.get(&name) else {
                let o = old.expect("name came from one side");
                out.schema.push(SchemaChange::DropTable { table: name.clone(), sql: o.info.sql.clone() });
                continue;
            };
            match old {
                None => out.schema.push(SchemaChange::CreateTable { table: name.clone(), sql: info.sql.clone() }),
                Some(o) if o.info.sql != info.sql => out.schema.push(SchemaChange::AlterTable {
                    table: name.clone(),
                    old_sql: o.info.sql.clone(),
                    new_sql: info.sql.clone(),
                    renamed: detect_rename(&o.info, info),
                }),
                _ => {}
            }
            let empty = TableState { info: info.clone(), structure: Structure { is_table: !info.without_rowid, ..Default::default() }, overflow_owner: HashMap::new() };
            let old = old.unwrap_or(&empty);
            let needs_walk = old.structure.all.is_empty()
                || old.info.root != info.root
                || old.info.without_rowid != info.without_rowid
                || dirty.contains(&info.root)
                || old.structure.interior.iter().any(|p| dirty.contains(p));
            let structure = if needs_walk {
                self.stats.rewalks += 1;
                btree::walk(&after, info.root)?
            } else {
                old.structure.clone()
            };
            // Cells whose overflow chain changed although the cell's own page
            // did not: SQLite overwrites same-size payloads in place.
            let mut dirty_t = dirty.clone();
            for (o, owner) in &old.overflow_owner {
                if dirty.contains(o) {
                    dirty_t.insert(*owner);
                }
            }
            let eb = old.structure.entry_pages();
            let ea = structure.entry_pages();
            let decode_before: BTreeSet<u32> = eb.difference(&ea).chain(eb.intersection(&dirty_t)).copied().collect();
            let decode_after: BTreeSet<u32> = ea.difference(&eb).chain(ea.intersection(&dirty_t)).copied().collect();
            self.stats.pages_decoded += (decode_before.len() + decode_after.len()) as u64;

            let mut overflow_owner = old.overflow_owner.clone();
            overflow_owner.retain(|_, owner| !decode_before.contains(owner));
            // Before images use the after schema's names: a rename in the same
            // unit as value changes must still compare by the new name.
            let rename = detect_rename(&old.info, info);
            let mut rb: HashMap<String, (Vec<Value>, Row)> = HashMap::new();
            for &p in &decode_before {
                for e in btree::entries(&before, p, enc_before)? {
                    let (k, mut row) = keyed_row(&old.info, &e);
                    if let Some((from, to)) = &rename {
                        for (n, _) in row.iter_mut() {
                            if n == from {
                                *n = to.clone();
                            }
                        }
                    }
                    rb.insert(key_json(&k), (k, row));
                }
            }
            let mut ra: HashMap<String, (Vec<Value>, Row)> = HashMap::new();
            for &p in &decode_after {
                for e in btree::entries(&after, p, enc_after)? {
                    for o in &e.overflow {
                        overflow_owner.insert(*o, p);
                    }
                    let (k, row) = keyed_row(info, &e);
                    ra.insert(key_json(&k), (k, row));
                }
            }
            for (kj, (k, b)) in &rb {
                match ra.get(kj) {
                    None => out.rows.push(RowChange { table: name.clone(), op: Op::Delete, key: k.clone(), before: Some(b.clone()), after: None }),
                    Some((_, a)) if differs(b, a, info) => out.rows.push(RowChange {
                        table: name.clone(),
                        op: Op::Update,
                        key: k.clone(),
                        before: Some(b.clone()),
                        after: Some(a.clone()),
                    }),
                    Some(_) => {}
                }
            }
            for (kj, (k, a)) in ra {
                if !rb.contains_key(&kj) {
                    out.rows.push(RowChange { table: name.clone(), op: Op::Insert, key: k, before: None, after: Some(a) });
                }
            }
            new_tables.insert(name, TableState { info: info.clone(), structure, overflow_owner });
        }

        for (pgno, data) in overlay {
            self.pages.pages.insert(pgno, data);
        }
        self.pages.pages.retain(|&p, _| p <= commit);
        self.pages.commit = commit;
        self.tables = new_tables;
        self.cookie = new_cookie;
        out.rows.sort_by(|a, b| (a.table.as_str(), a.key_json()).cmp(&(b.table.as_str(), b.key_json())));
        Ok(out)
    }

    fn encoding_after(&self, overlay: &HashMap<u32, Vec<u8>>) -> crate::format::TextEncoding {
        overlay.get(&1).map_or_else(|| self.pages.encoding(), |p| crate::format::TextEncoding::from_header(p))
    }
}

/// Key and named row of one entry. Key: declared PK (rowid alias → rowid); no
/// PK → rowid; a declared PK containing NULL (allowed in rowid tables) falls
/// back to the rowid so keys stay unique.
fn keyed_row(info: &TableInfo, e: &btree::Entry) -> (Vec<Value>, Row) {
    let values = info.row(e.rowid, &e.record);
    let key = if info.pk.is_empty() {
        vec![Value::Integer(e.rowid.unwrap_or_default())]
    } else {
        let k: Vec<Value> = info.pk.iter().map(|&c| values[c].clone()).collect();
        if !info.without_rowid && k.iter().any(Value::is_null) {
            vec![Value::Text(format!("$rowid:{}", e.rowid.unwrap_or_default()))]
        } else {
            k
        }
    };
    let row = info.columns.iter().zip(values).map(|(c, v)| (c.name.clone(), v)).collect();
    (key, row)
}

/// Value comparison by column name over the after schema; a column missing
/// from the before image compares against its default (design §6).
fn differs(before: &Row, after: &Row, info: &TableInfo) -> bool {
    if before.len() == after.len() && before.iter().zip(after).all(|(b, a)| b.0 == a.0) {
        return before.iter().zip(after).any(|(b, a)| b.1 != a.1);
    }
    let b: HashMap<&str, &Value> = before.iter().map(|(n, v)| (n.as_str(), v)).collect();
    after.iter().zip(&info.columns).any(|((n, a), col)| b.get(n.as_str()).copied().unwrap_or(&col.default) != a)
}

fn image_with(pages: &Pages, overlay: &HashMap<u32, Vec<u8>>, commit: u32) -> Vec<u8> {
    let mut merged = Pages { page_size: pages.page_size, usable: pages.usable, commit, pages: pages.pages.clone() };
    for (p, d) in overlay {
        merged.pages.insert(*p, d.clone());
    }
    merged.to_image()
}
