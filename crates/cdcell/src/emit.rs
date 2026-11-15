//! Row changes → change-log rows (design §2).
//!
//! One append-only table per (script, class, SQLite table). Meta columns
//! (double underscore):
//!
//! | column | meaning |
//! |---|---|
//! | `__cell_id` | the Durable Object, `Class:<64 hex>` |
//! | `__epoch`, `__txid` | truthful order; compare as the pair |
//! | `__op` | `c` / `u` / `d` / `r` (bootstrap) |
//! | `__key` | canonical JSON array of the primary key |
//! | `__ts` | capture time (owner wall clock; display only) |
//! | `__schema_version` | the cell's table schema version (registry) |
//! | `__ingested_at` | when cdcell produced the row |
//! | `__raw` | values that do not fit their column type, JSON-encoded |
//!
//! then the table's own columns: after values for c/u/r, only the key
//! columns for d. User columns starting with `__` are written as `u__…`.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use cdcell_core::schema::{Affinity, TableInfo};
use cdcell_core::value::key_json;
use cdcell_core::{Op, RowChange, Value};

use crate::iceberg_out::{OutRow, OutValue};

/// Row key holding the table-schema fingerprint until the committer maps
/// it to `__schema_version` (never written).
pub const FINGERPRINT: &str = "\u{0}fingerprint";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Streamed,
    /// Re-emission after a resolution or rebuild (design §4, §5).
    Synthetic,
    /// Initial state of a cell with no committed history.
    Bootstrap,
}

#[derive(Clone, Debug)]
pub struct Ctx<'a> {
    pub cell: &'a str,
    pub epoch: u64,
    pub txid: u64,
    /// LTX header capture time, ms.
    pub capture_ms: i64,
    pub now_us: i64,
    pub kind: Kind,
}

/// Output column name for a SQLite column (meta columns are reserved).
pub fn out_col(name: &str) -> String {
    if name.starts_with("__") {
        format!("u{name}")
    } else {
        name.to_string()
    }
}

/// Fingerprint of a table schema: the normalized column list (design
/// rev 5 schema registry). Cells that reached the same columns through
/// different migrations share it.
pub fn fingerprint(info: &TableInfo) -> String {
    let mut h = Sha256::new();
    h.update(if info.without_rowid {
        b"wr;" as &[u8]
    } else {
        b"rowid;"
    });
    for (i, c) in info.columns.iter().enumerate() {
        let pk = info.pk.iter().position(|&p| p == i).map_or(0, |p| p + 1);
        h.update(
            format!(
                "{}|{}|{}|{}|{}|{:?};",
                c.name,
                c.decl_type.trim().to_ascii_uppercase(),
                pk,
                c.not_null,
                c.default.to_json(),
                c.generated
            )
            .as_bytes(),
        );
    }
    h.finalize()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// SQLite value → typed column value, or `None` when it does not fit the
/// column's affinity (it then goes to `__raw`, design §2).
pub(crate) fn typed(v: &Value, aff: Affinity) -> Option<OutValue> {
    const EXACT: i64 = 1 << 53;
    Some(match (aff, v) {
        (_, Value::Null) => OutValue::Null,
        (Affinity::Integer, Value::Integer(i)) => OutValue::Long(*i),
        (Affinity::Real | Affinity::Numeric, Value::Real(f)) => OutValue::Double(*f),
        (Affinity::Real | Affinity::Numeric, Value::Integer(i)) if i.abs() <= EXACT => {
            OutValue::Double(*i as f64)
        }
        (Affinity::Text, Value::Text(s)) => OutValue::Str(s.clone()),
        (Affinity::Blob, Value::Blob(b)) => OutValue::Bin(b.clone()),
        (Affinity::Untyped, v) => OutValue::Str(v.to_json()),
        _ => return None,
    })
}

fn meta(ctx: &Ctx<'_>, op: &str, pk: &str) -> OutRow {
    let mut r: OutRow = BTreeMap::new();
    r.insert("__cell_id".into(), OutValue::Str(ctx.cell.to_string()));
    r.insert("__epoch".into(), OutValue::Long(ctx.epoch as i64));
    r.insert("__txid".into(), OutValue::Long(ctx.txid as i64));
    r.insert("__op".into(), OutValue::Str(op.to_string()));
    r.insert("__key".into(), OutValue::Str(pk.to_string()));
    r.insert("__ts".into(), OutValue::Ts(ctx.capture_ms * 1000));
    r.insert("__ingested_at".into(), OutValue::Ts(ctx.now_us));
    r
}

fn op_code(ctx: &Ctx<'_>, c: &RowChange) -> &'static str {
    match ctx.kind {
        Kind::Bootstrap => "r",
        _ => c.op.code(),
    }
}

/// Change-log rows for one unit's changes to ordinary tables.
pub fn rows(
    ctx: &Ctx<'_>,
    infos: &dyn Fn(&str) -> Option<TableInfo>,
    changes: &[RowChange],
) -> Vec<(String, OutRow)> {
    let mut out = Vec::with_capacity(changes.len());
    for c in changes {
        let Some(info) = infos(&c.table) else {
            continue;
        };
        let pk = key_json(&c.key);
        let mut r = meta(ctx, op_code(ctx, c), &pk);
        r.insert(FINGERPRINT.into(), OutValue::Str(fingerprint(&info)));
        let mut raw = Vec::new();
        let (image, key_only) = match (c.op, &c.after, &c.before) {
            (Op::Delete, _, Some(b)) => (b, true),
            (_, Some(a), _) => (a, false),
            _ => continue,
        };
        for (name, v) in image {
            let Some((idx, col)) = info
                .columns
                .iter()
                .enumerate()
                .find(|(_, col)| &col.name == name)
            else {
                continue;
            };
            if key_only && !info.pk.contains(&idx) {
                continue; // deletes carry only the key
            }
            match typed(v, col.affinity) {
                Some(t) => {
                    r.insert(out_col(name), t);
                }
                None => raw.push((out_col(name), v.to_json())),
            }
        }
        if !raw.is_empty() {
            r.insert("__raw".into(), OutValue::Map(raw));
        }
        out.push((c.table.clone(), r));
    }
    out
}

/// A delete row built from a change-log key (resync, design §6): meta,
/// fingerprint and the key columns that can be typed.
pub fn delete_row(
    ctx: &Ctx<'_>,
    key: &str,
    fingerprint: &str,
    cols: Vec<(String, OutValue)>,
) -> OutRow {
    let mut r = meta(ctx, "d", key);
    r.insert(FINGERPRINT.into(), OutValue::Str(fingerprint.to_string()));
    for (c, v) in cols {
        r.insert(c, v);
    }
    r
}

/// The KV table (design §2).
pub const KV_TABLE: &str = "_cdcell_kv";

/// `_cf_KV` changes → `_cdcell_kv` rows keyed by `k` (one scope per DB):
/// columns `k`, `value_type`, `value_json`, `value_raw`.
pub fn kv_rows(ctx: &Ctx<'_>, changes: &[RowChange]) -> Vec<(String, OutRow)> {
    let mut out = Vec::with_capacity(changes.len());
    for c in changes {
        let k = match c.key.last() {
            Some(Value::Text(k)) => k.clone(),
            _ => continue,
        };
        let pk = key_json(&[Value::Text(k.clone())]);
        let mut r = meta(ctx, op_code(ctx, c), &pk);
        r.insert(FINGERPRINT.into(), OutValue::Str("kv-v1".into()));
        r.insert("k".into(), OutValue::Str(k));
        if c.op != Op::Delete {
            if let Some(a) = &c.after {
                let v = a
                    .iter()
                    .find(|(n, _)| n == "v")
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Null);
                let d = cdcell_core::kv::decode(&v);
                r.insert(
                    "value_type".into(),
                    OutValue::Str(d.value_type.as_str().into()),
                );
                r.insert(
                    "value_json".into(),
                    d.value_json.map_or(OutValue::Null, OutValue::Str),
                );
                r.insert("value_raw".into(), OutValue::Bin(d.value_raw));
            }
        }
        out.push((KV_TABLE.to_string(), r));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typing_and_names() {
        assert_eq!(
            typed(&Value::Integer(5), Affinity::Real),
            Some(OutValue::Double(5.0))
        );
        assert_eq!(typed(&Value::Text("x".into()), Affinity::Integer), None);
        assert_eq!(
            typed(&Value::Integer(5), Affinity::Untyped),
            Some(OutValue::Str("5".into()))
        );
        assert_eq!(out_col("__op"), "u__op");
        assert_eq!(out_col("note"), "note");
    }
}
