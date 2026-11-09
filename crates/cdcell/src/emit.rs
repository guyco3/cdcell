//! Row changes → output rows (design §8.2-§8.5, §8.9).

use std::collections::BTreeMap;

use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use sha2::{Digest, Sha256};

use cdcell_core::schema::{Affinity, TableInfo};
use cdcell_core::value::key_json;
use cdcell_core::{Op, RowChange, Value};

use crate::iceberg_out::{OutRow, OutValue};

pub const CONNECTOR_VERSION: &str = env!("CARGO_PKG_VERSION");

/// What the event is (design §8.4, §8.5 id namespaces).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Streamed,
    Synthetic,
    Bootstrap { first: bool, last: bool },
}

/// Per-unit context shared by the unit's events.
#[derive(Clone, Debug)]
pub struct Ctx<'a> {
    pub script: &'a str,
    pub class: &'a str,
    pub cell: &'a str,
    pub epoch: u64,
    pub txid: u64,
    /// LTX header capture time, ms.
    pub capture_ms: i64,
    /// Processing time, µs.
    pub now_us: i64,
    pub ce_type_prefix: &'a str,
    pub schema_version: u32,
    pub kind: Kind,
}

const PATH_SEG: &AsciiSet = &CONTROLS.add(b'/').add(b'%').add(b' ');
const SUBJECT: &AsciiSet = &CONTROLS.add(b'/').add(b'%').add(b' ').add(b'"').add(b'[').add(b']').add(b',').add(b'{').add(b'}').add(b':');

pub fn ce_source(script: &str, class: &str, cell: &str) -> String {
    format!("/cdcell/sqlite/{}/{}/{}", utf8_percent_encode(script, PATH_SEG), utf8_percent_encode(class, PATH_SEG), utf8_percent_encode(cell, PATH_SEG))
}

pub fn ce_id(kind: Kind, epoch: u64, txid: u64, table: &str, pk_json: &str) -> String {
    let h = Sha256::digest(pk_json.as_bytes());
    let hex: String = h[..8].iter().map(|b| format!("{b:02x}")).collect();
    let prefix = match kind {
        Kind::Streamed => "",
        Kind::Synthetic => "s:",
        Kind::Bootstrap { .. } => "r:",
    };
    format!("{prefix}{epoch}:{txid}:{table}:{hex}")
}

/// SQLite value → typed column value, or `None` when it does not fit the
/// column type (it then goes to `_raw`, design §8.3).
fn typed(v: &Value, aff: Affinity) -> Option<OutValue> {
    const EXACT: i64 = 1 << 53;
    Some(match (aff, v) {
        (_, Value::Null) => OutValue::Null,
        (Affinity::Integer, Value::Integer(i)) => OutValue::Long(*i),
        (Affinity::Real | Affinity::Numeric, Value::Real(f)) => OutValue::Double(*f),
        (Affinity::Real | Affinity::Numeric, Value::Integer(i)) if i.abs() <= EXACT => OutValue::Double(*i as f64),
        (Affinity::Text, Value::Text(s)) => OutValue::Str(s.clone()),
        (Affinity::Blob, Value::Blob(b)) => OutValue::Bin(b.clone()),
        (Affinity::Untyped, v) => OutValue::Str(v.to_json()),
        _ => return None,
    })
}

fn image(info: &TableInfo, row: &[(String, Value)], raw: &mut Vec<(String, String)>, record_raw: bool) -> OutValue {
    let mut m = BTreeMap::new();
    for (name, v) in row {
        let aff = info.columns.iter().find(|c| &c.name == name).map_or(Affinity::Untyped, |c| c.affinity);
        match typed(v, aff) {
            Some(t) => {
                m.insert(name.clone(), t);
            }
            None => {
                m.insert(name.clone(), OutValue::Null);
                if record_raw {
                    raw.push((name.clone(), v.to_json()));
                }
            }
        }
    }
    OutValue::Struct(m)
}

fn key_struct(info: &TableInfo, key: &[Value]) -> OutValue {
    let mut m = BTreeMap::new();
    if info.pk.is_empty() {
        m.insert("rowid".into(), match key.first() {
            Some(Value::Integer(i)) => OutValue::Long(*i),
            _ => OutValue::Null,
        });
    } else if key.len() == info.pk.len() {
        for (&c, v) in info.pk.iter().zip(key) {
            let col = &info.columns[c];
            m.insert(col.name.clone(), typed(v, col.affinity).unwrap_or(OutValue::Null));
        }
    } else {
        return OutValue::Null; // rowid fallback for a NULL PK: `_key_json` carries it
    }
    OutValue::Struct(m)
}

/// Builds the output rows for one unit. `changes` must be the unit's row
/// changes for one table set, sorted by (table, pk_json) (the decoder's
/// order), which fixes `transaction.total_order`.
pub fn rows(ctx: &Ctx<'_>, infos: &dyn Fn(&str) -> Option<TableInfo>, changes: &[RowChange]) -> Vec<(String, OutRow)> {
    let mut out = Vec::with_capacity(changes.len());
    let mut per_table: BTreeMap<&str, i64> = BTreeMap::new();
    let source = ce_source(ctx.script, ctx.class, ctx.cell);
    let partition = format!("{}/{}/{}", ctx.script, ctx.class, ctx.cell);
    let n = changes.len();
    for (i, c) in changes.iter().enumerate() {
        let Some(info) = infos(&c.table) else { continue };
        let total = i as i64 + 1;
        let dco = {
            let e = per_table.entry(c.table.as_str()).or_default();
            *e += 1;
            *e
        };
        let pk = key_json(&c.key);
        let mut raw = Vec::new();
        let before = c.before.as_ref().map(|r| image(&info, r, &mut raw, c.after.is_none()));
        let after = c.after.as_ref().map(|r| image(&info, r, &mut raw, true));
        let (op, snapshot) = match ctx.kind {
            Kind::Bootstrap { first, last } => ("r", if first { "first" } else if last && i + 1 == n { "last" } else { "true" }),
            _ => (c.op.code(), "false"),
        };
        let kind = ctx.kind;
        let mut r: OutRow = BTreeMap::new();
        let s = |v: &str| OutValue::Str(v.to_string());
        r.insert("ce_id".into(), s(&ce_id(kind, ctx.epoch, ctx.txid, &c.table, &pk)));
        r.insert("ce_source".into(), s(&source));
        r.insert("ce_specversion".into(), s("1.0"));
        r.insert("ce_type".into(), s(&format!("{}.cdcell.sqlite.DataChangeEvent", ctx.ce_type_prefix)));
        r.insert("ce_subject".into(), s(&format!("{}/{}", c.table, utf8_percent_encode(&pk, SUBJECT))));
        r.insert("ce_time".into(), OutValue::Ts(ctx.capture_ms * 1000));
        r.insert("ce_datacontenttype".into(), s("application/json"));
        r.insert("ce_dataschema".into(), s(&format!("urn:cdcell:schema:{}:{}:{}:{}", ctx.script, ctx.class, c.table, ctx.schema_version)));
        r.insert("ce_sequence".into(), s(&format!("{:020}{:020}{:010}", ctx.epoch, ctx.txid, total)));
        r.insert("ce_partitionkey".into(), s(&partition));
        r.insert("ce_recordedtime".into(), OutValue::Ts(ctx.now_us));
        r.insert("_cell_id".into(), s(ctx.cell));
        r.insert("_epoch".into(), OutValue::Long(ctx.epoch as i64));
        r.insert("_txid".into(), OutValue::Long(ctx.txid as i64));
        r.insert("_key_json".into(), s(&pk));
        r.insert("_key".into(), key_struct(&info, &c.key));
        r.insert("op".into(), s(op));
        if let Some(b) = before {
            r.insert("before".into(), b);
        }
        if let Some(a) = after {
            r.insert("after".into(), a);
        }
        if !raw.is_empty() {
            r.insert("_raw".into(), OutValue::Map(raw));
        }
        let mut src = BTreeMap::new();
        src.insert("version".into(), s(CONNECTOR_VERSION));
        src.insert("connector".into(), s("cdcell"));
        src.insert("name".into(), s(ctx.script));
        src.insert("ts_ms".into(), OutValue::Long(ctx.capture_ms));
        src.insert("ts_us".into(), OutValue::Long(ctx.capture_ms * 1000));
        src.insert("snapshot".into(), s(snapshot));
        src.insert("db".into(), s(ctx.cell));
        src.insert("sequence".into(), s(&format!("[\"{}:{}\"]", ctx.epoch, ctx.txid)));
        src.insert("table".into(), s(&c.table));
        src.insert("change_id".into(), OutValue::Long(ctx.txid as i64));
        src.insert("script".into(), s(ctx.script));
        src.insert("cell_class".into(), s(ctx.class));
        src.insert("cell_id".into(), s(ctx.cell));
        src.insert("epoch".into(), OutValue::Long(ctx.epoch as i64));
        src.insert("txid".into(), OutValue::Long(ctx.txid as i64));
        src.insert("synthetic".into(), OutValue::Bool(matches!(kind, Kind::Synthetic)));
        r.insert("source".into(), OutValue::Struct(src));
        let mut tx = BTreeMap::new();
        tx.insert("id".into(), s(&format!("{}:{}", ctx.epoch, ctx.txid)));
        tx.insert("total_order".into(), OutValue::Long(total));
        tx.insert("data_collection_order".into(), OutValue::Long(dco));
        r.insert("transaction".into(), OutValue::Struct(tx));
        r.insert("ts_ms".into(), OutValue::Long(ctx.now_us / 1000));
        r.insert("ts_us".into(), OutValue::Long(ctx.now_us));
        let _ = Op::Insert;
        out.push((c.table.clone(), r));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_formats() {
        assert_eq!(ce_source("shop", "Shop", "Shop:abc"), "/cdcell/sqlite/shop/Shop/Shop:abc");
        let id = ce_id(Kind::Streamed, 9, 4182, "orders", "[42]");
        assert!(id.starts_with("9:4182:orders:") && id.len() == "9:4182:orders:".len() + 16);
        assert!(ce_id(Kind::Synthetic, 1, 1, "t", "[1]").starts_with("s:"));
        assert_eq!(typed(&Value::Integer(5), Affinity::Real), Some(OutValue::Double(5.0)));
        assert_eq!(typed(&Value::Text("x".into()), Affinity::Integer), None);
        assert_eq!(typed(&Value::Integer(5), Affinity::Untyped), Some(OutValue::Str("5".into())));
    }
}
