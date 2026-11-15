//! Resync from a later full image (design §6): the cell's committed point
//! is no longer reconstructible (its bundles are GC'd and celld folded the
//! history into an L1 that ends beyond it). The image gives the live rows;
//! the cell's own change log gives the keys that existed at the committed
//! point, so keys missing from the image are emitted as deletes. The result
//! is one net change at the image's (epoch, txid).

use std::collections::{BTreeSet, HashMap};

use crate::iceberg_out::OutValue;

/// Keys whose latest row by `(epoch, txid)` is not a delete.
/// Rows: `(key, op, epoch, txid)`.
pub fn live_keys(rows: impl IntoIterator<Item = (String, String, i64, i64)>) -> BTreeSet<String> {
    let mut latest: HashMap<String, (i64, i64, bool)> = HashMap::new();
    for (k, op, e, t) in rows {
        let del = op == "d";
        match latest.get(&k) {
            Some(&(le, lt, _)) if (le, lt) >= (e, t) => {}
            _ => {
                latest.insert(k, (e, t, del));
            }
        }
    }
    latest
        .into_iter()
        .filter(|(_, (_, _, del))| !del)
        .map(|(k, _)| k)
        .collect()
}

/// Keys live in the change log but absent from the image: owed deletes.
pub fn owed_deletes(live: &BTreeSet<String>, image: &BTreeSet<String>) -> Vec<String> {
    live.difference(image).cloned().collect()
}

/// A canonical JSON key array → typed key values (`None` where the JSON
/// value has no column representation, e.g. a base64 blob object).
pub fn key_values(key: &str) -> Vec<Option<OutValue>> {
    let Ok(serde_json::Value::Array(vs)) = serde_json::from_str::<serde_json::Value>(key) else {
        return Vec::new();
    };
    vs.iter()
        .map(|v| match v {
            serde_json::Value::Number(n) if n.is_i64() => n.as_i64().map(OutValue::Long),
            serde_json::Value::Number(n) => n.as_f64().map(OutValue::Double),
            serde_json::Value::String(s) => Some(OutValue::Str(s.clone())),
            _ => None,
        })
        .collect()
}

/// Typed key columns for a delete row built from a change-log key: each PK
/// value typed by its column's affinity (as emit does). A key whose shape
/// does not match the table's PK (rowid tables, the `$rowid:N` fallback, a
/// changed PK) or a value that does not fit its column yields no columns;
/// `__key` alone identifies the row.
pub fn key_columns(info: &cdcell_core::schema::TableInfo, key: &str) -> Vec<(String, OutValue)> {
    use cdcell_core::Value;
    let Ok(serde_json::Value::Array(vs)) = serde_json::from_str::<serde_json::Value>(key) else {
        return Vec::new();
    };
    if info.pk.is_empty() || vs.len() != info.pk.len() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (&i, v) in info.pk.iter().zip(&vs) {
        let value = match v {
            serde_json::Value::Number(n) if n.is_i64() => Value::Integer(n.as_i64().unwrap()),
            serde_json::Value::Number(n) => match n.as_f64() {
                Some(f) => Value::Real(f),
                None => return Vec::new(),
            },
            serde_json::Value::String(s) if s.starts_with("$rowid:") => return Vec::new(),
            serde_json::Value::String(s) => Value::Text(s.clone()),
            _ => return Vec::new(),
        };
        let col = &info.columns[i];
        match crate::emit::typed(&value, col.affinity) {
            Some(t) => out.push((crate::emit::out_col(&col.name), t)),
            None => return Vec::new(),
        }
    }
    out
}

/// One object or buffered unit of a cell: `epoch`, TXID range, and where
/// its pages come from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Span<T> {
    pub epoch: u64,
    pub min: u64,
    pub max: u64,
    pub src: T,
}

/// celld's restore chain (its `EpochChain`) over a cell's spans: start at
/// epoch `top` and walk down to the first epoch with a span opening at
/// TXID 1 (the base); an epoch without one is paged and opens at its lowest
/// span's `min` = the cut + 1 of the next lower epoch that has spans, whose
/// path must end exactly at that cut (spans above it are a fenced owner's
/// tail, not history). Within an epoch the path is greedy: the base (or
/// opener) with the largest reach, then contiguous spans.
///
/// `cap` bounds the top epoch (a committed point). Returns the spans to
/// apply, oldest first, and the TXID reached in `top` (≤ `cap`, possibly
/// lower when the exact point is no longer reconstructible), or `None`
/// when no chain exists yet.
pub fn plan_chain<T: Clone>(
    spans: &[Span<T>],
    top: u64,
    cap: Option<u64>,
) -> Option<(Vec<Span<T>>, u64)> {
    let mut epochs_rev: Vec<Vec<Span<T>>> = Vec::new();
    let (mut k, mut bound, mut exact) = (top, cap, false);
    let mut top_reached = 0;
    loop {
        let in_k: Vec<&Span<T>> = spans
            .iter()
            .filter(|s| s.epoch == k && bound.is_none_or(|c| s.max <= c))
            .collect();
        let base = in_k.iter().filter(|s| s.min == 1).max_by_key(|s| s.max);
        let first = match base {
            Some(b) => *b,
            None => in_k
                .iter()
                .min_by_key(|s| (s.min, std::cmp::Reverse(s.max)))?,
        };
        let mut path = vec![first.clone()];
        let mut cur = first.max;
        while let Some(n) = in_k
            .iter()
            .filter(|s| s.min <= cur + 1 && s.max > cur)
            .max_by_key(|s| s.max)
        {
            cur = n.max;
            path.push((*n).clone());
        }
        if exact && Some(cur) != bound {
            return None;
        }
        if k == top {
            top_reached = cur;
        }
        epochs_rev.push(path);
        if base.is_some() {
            break;
        }
        // paged: the next lower epoch with spans must end exactly at the cut
        let cut = first.min.checked_sub(1).filter(|&c| c > 0)?;
        k = spans
            .iter()
            .filter(|s| s.epoch < k)
            .map(|s| s.epoch)
            .max()?;
        bound = Some(cut);
        exact = true;
    }
    epochs_rev.reverse();
    Some((epochs_rev.into_iter().flatten().collect(), top_reached))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(k: &str, op: &str, e: i64, t: i64) -> (String, String, i64, i64) {
        (k.into(), op.into(), e, t)
    }

    #[test]
    fn live_keys_follow_latest_version() {
        let rows = vec![
            r("[1]", "c", 1, 5),
            r("[1]", "d", 1, 9), // deleted
            r("[2]", "c", 1, 5),
            r("[2]", "d", 1, 6),
            r("[2]", "c", 2, 1), // re-created in a later epoch
            r("[3]", "u", 1, 7),
            r("[3]", "u", 1, 7), // crash duplicate
            r("[4]", "d", 1, 3),
            r("[4]", "c", 1, 2), // out of file order: delete is later
        ];
        let live = live_keys(rows);
        assert_eq!(live.into_iter().collect::<Vec<_>>(), vec!["[2]", "[3]"]);
    }

    #[test]
    fn deletes_owed_against_image() {
        let live: BTreeSet<String> = ["[1]", "[2]", "[3]"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let image: BTreeSet<String> = ["[2]", "[5]"].iter().map(|s| s.to_string()).collect();
        assert_eq!(owed_deletes(&live, &image), vec!["[1]", "[3]"]);
    }

    fn sp(epoch: u64, min: u64, max: u64) -> Span<&'static str> {
        Span {
            epoch,
            min,
            max,
            src: "",
        }
    }
    type Plan = Option<(Vec<Span<&'static str>>, u64)>;
    type Ranges = Option<(Vec<(u64, u64, u64)>, u64)>;
    fn ranges(p: &Plan) -> Ranges {
        p.as_ref()
            .map(|(v, r)| (v.iter().map(|s| (s.epoch, s.min, s.max)).collect(), *r))
    }

    #[test]
    fn chain_single_epoch() {
        // L1 1-10 plus L0s 11, 12; an older snapshot 1-1 is superseded.
        let s = vec![sp(1, 1, 1), sp(1, 1, 10), sp(1, 11, 11), sp(1, 12, 12)];
        assert_eq!(
            ranges(&plan_chain(&s, 1, None)),
            Some((vec![(1, 1, 10), (1, 11, 11), (1, 12, 12)], 12))
        );
    }

    #[test]
    fn chain_cap_exact_lower_or_none() {
        let s = vec![sp(1, 1, 10), sp(1, 11, 11), sp(1, 12, 12)];
        assert_eq!(
            ranges(&plan_chain(&s, 1, Some(11))),
            Some((vec![(1, 1, 10), (1, 11, 11)], 11))
        );
        // 13 is not there yet: the best point below the cap
        assert_eq!(
            ranges(&plan_chain(&s, 1, Some(13))),
            Some((vec![(1, 1, 10), (1, 11, 11), (1, 12, 12)], 12))
        );
        // the only base reaches past the cap (an L1 fold): nothing at or below it
        let folded = vec![sp(1, 1, 256)];
        assert_eq!(ranges(&plan_chain(&folded, 1, Some(51))), None);
    }

    #[test]
    fn chain_paged_across_epochs() {
        // e2 is paged: opens at 21 (= e1's cut 20 + 1); e1 has a fenced
        // owner's tail past the cut (21, 22) that is not history.
        let s = vec![
            sp(1, 1, 20),
            sp(1, 21, 21),
            sp(1, 22, 22),
            sp(2, 21, 21),
            sp(2, 22, 22),
            sp(2, 23, 23),
        ];
        assert_eq!(
            ranges(&plan_chain(&s, 2, None)),
            Some((vec![(1, 1, 20), (2, 21, 21), (2, 22, 22), (2, 23, 23)], 23))
        );
    }

    #[test]
    fn chain_through_intermediate_paged_epochs_and_gcd_ones() {
        // e3 paged at 31 over e2 paged at 21 over e1's base; e0 GC'd
        let s = vec![sp(1, 1, 20), sp(2, 21, 30), sp(3, 31, 31)];
        assert_eq!(
            ranges(&plan_chain(&s, 3, None)),
            Some((vec![(1, 1, 20), (2, 21, 30), (3, 31, 31)], 31))
        );
        // the owner's own epoch has a TXID-1 image: lower epochs don't matter
        let s = vec![sp(5, 1, 1), sp(5, 2, 2)];
        assert_eq!(
            ranges(&plan_chain(&s, 5, None)),
            Some((vec![(5, 1, 1), (5, 2, 2)], 2))
        );
    }

    #[test]
    fn chain_none_without_base_or_exact_cut() {
        // paged top, base epoch GC'd
        assert_eq!(ranges(&plan_chain(&[sp(2, 21, 22)], 2, None)), None);
        // lower epoch does not reach the cut exactly
        assert_eq!(
            ranges(&plan_chain(&[sp(1, 1, 18), sp(2, 21, 22)], 2, None)),
            None
        );
        // nothing in the top epoch
        assert_eq!(ranges(&plan_chain(&[sp(1, 1, 5)], 2, None)), None);
    }

    fn table(
        cols: &[(&str, cdcell_core::schema::Affinity)],
        pk: &[usize],
    ) -> cdcell_core::schema::TableInfo {
        cdcell_core::schema::TableInfo {
            name: "t".into(),
            root: 2,
            sql: String::new(),
            without_rowid: false,
            columns: cols
                .iter()
                .map(|(n, a)| cdcell_core::schema::Column {
                    name: n.to_string(),
                    decl_type: String::new(),
                    affinity: *a,
                    not_null: false,
                    default: cdcell_core::Value::Null,
                    generated: None,
                })
                .collect(),
            record_order: (0..cols.len()).collect(),
            rowid_alias: None,
            pk: pk.to_vec(),
        }
    }

    #[test]
    fn key_columns_typed_by_affinity() {
        use cdcell_core::schema::Affinity::*;
        let t = table(&[("order_id", Integer), ("sku", Text)], &[0, 1]);
        assert_eq!(
            key_columns(&t, r#"[7,"a"]"#),
            vec![
                ("order_id".into(), OutValue::Long(7)),
                ("sku".into(), OutValue::Str("a".into()))
            ]
        );
        // untyped PK column: canonical JSON string, like emit
        let u = table(&[("k", Untyped)], &[0]);
        assert_eq!(
            key_columns(&u, "[5]"),
            vec![("k".into(), OutValue::Str("5".into()))]
        );
        // shape or type mismatch: `__key` only
        assert_eq!(key_columns(&t, "[7]"), vec![]);
        assert_eq!(
            key_columns(&table(&[("id", Integer)], &[0]), r#"["$rowid:3"]"#),
            vec![]
        );
        assert_eq!(
            key_columns(&table(&[("id", Integer)], &[0]), r#"["x"]"#),
            vec![]
        );
        assert_eq!(key_columns(&table(&[("id", Integer)], &[]), "[3]"), vec![]);
    }

    #[test]
    fn key_values_parse_canonical_keys() {
        assert_eq!(key_values("[42]"), vec![Some(OutValue::Long(42))]);
        assert_eq!(
            key_values(r#"["a",1.5]"#),
            vec![Some(OutValue::Str("a".into())), Some(OutValue::Double(1.5))]
        );
        assert_eq!(key_values(r#"[{"$b64":"AA=="}]"#), vec![None]);
    }
}
