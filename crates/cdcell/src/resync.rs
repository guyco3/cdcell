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
    latest.into_iter().filter(|(_, (_, _, del))| !del).map(|(k, _)| k).collect()
}

/// Keys live in the change log but absent from the image: owed deletes.
pub fn owed_deletes(live: &BTreeSet<String>, image: &BTreeSet<String>) -> Vec<String> {
    live.difference(image).cloned().collect()
}

/// A canonical JSON key array → typed key values (`None` where the JSON
/// value has no column representation, e.g. a base64 blob object).
pub fn key_values(key: &str) -> Vec<Option<OutValue>> {
    let Ok(serde_json::Value::Array(vs)) = serde_json::from_str::<serde_json::Value>(key) else { return Vec::new() };
    vs.iter()
        .map(|v| match v {
            serde_json::Value::Number(n) if n.is_i64() => n.as_i64().map(OutValue::Long),
            serde_json::Value::Number(n) => n.as_f64().map(OutValue::Double),
            serde_json::Value::String(s) => Some(OutValue::Str(s.clone())),
            _ => None,
        })
        .collect()
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
            r("[1]", "d", 1, 9),  // deleted
            r("[2]", "c", 1, 5),
            r("[2]", "d", 1, 6),
            r("[2]", "c", 2, 1),  // re-created in a later epoch
            r("[3]", "u", 1, 7),
            r("[3]", "u", 1, 7),  // crash duplicate
            r("[4]", "d", 1, 3),
            r("[4]", "c", 1, 2),  // out of file order: delete is later
        ];
        let live = live_keys(rows);
        assert_eq!(live.into_iter().collect::<Vec<_>>(), vec!["[2]", "[3]"]);
    }

    #[test]
    fn deletes_owed_against_image() {
        let live: BTreeSet<String> = ["[1]", "[2]", "[3]"].iter().map(|s| s.to_string()).collect();
        let image: BTreeSet<String> = ["[2]", "[5]"].iter().map(|s| s.to_string()).collect();
        assert_eq!(owed_deletes(&live, &image), vec!["[1]", "[3]"]);
    }

    #[test]
    fn key_values_parse_canonical_keys() {
        assert_eq!(key_values("[42]"), vec![Some(OutValue::Long(42))]);
        assert_eq!(key_values(r#"["a",1.5]"#), vec![Some(OutValue::Str("a".into())), Some(OutValue::Double(1.5))]);
        assert_eq!(key_values(r#"[{"$b64":"AA=="}]"#), vec![None]);
    }
}
