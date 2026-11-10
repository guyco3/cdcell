//! M1 gate, part 2: real celld output.
//!
//! `tests/fixtures/<shop>/` holds every bundle row celld wrote for one shop
//! Durable Object (epoch 1, TXID 1 onward, captured by
//! e2e/tools/capture_fixture.py) and the object's own `?op=dump` taken at the
//! end. Applying the rows in order must reproduce the dump exactly, including
//! `retouch` updates that change only overflow bytes.

use std::collections::BTreeMap;
use std::path::Path;

use cdcell_core::{Decoder, Op, Value};
use serde_json::Value as J;

fn num(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Real(f) => *f,
        other => panic!("not a number: {other:?}"),
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(t) => t.clone(),
        other => panic!("not text: {other:?}"),
    }
}

fn col<'a>(row: &'a [(String, Value)], name: &str) -> &'a Value {
    &row.iter().find(|(n, _)| n == name).unwrap_or_else(|| panic!("no column {name}")).1
}

fn check_fixture(dir: &Path) {
    let meta: J = serde_json::from_str(&std::fs::read_to_string(dir.join("dump.json")).unwrap()).unwrap();
    let dump = &meta["dump"];
    let mut files: Vec<_> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).filter(|p| p.extension().is_some_and(|x| x == "ltx")).collect();
    files.sort();

    let mut dec = Decoder::new();
    let mut updates_by_table: BTreeMap<String, usize> = BTreeMap::new();
    let mut expect_txid = 1;
    for (i, f) in files.iter().enumerate() {
        let unit = cdcell_core::ltx::decode_file(&std::fs::read(f).unwrap()).unwrap();
        assert_eq!(unit.min_txid, expect_txid, "{}: TXIDs must be contiguous", f.display());
        assert_eq!(unit.is_snapshot(), i == 0, "only the first unit is a TXID-1 snapshot");
        expect_txid = unit.max_txid + 1;
        let changes = dec.apply(unit.pages, unit.commit).unwrap_or_else(|e| panic!("{}: {e}", f.display()));
        for c in changes.rows.iter().filter(|c| c.op == Op::Update) {
            *updates_by_table.entry(c.table.clone()).or_default() += 1;
        }
    }

    // orders
    let orders = dec.rows("orders").unwrap();
    let want = dump["orders"].as_array().unwrap();
    assert_eq!(orders.len(), want.len(), "orders row count");
    for w in want {
        let (_, row) = &orders[&format!("[{}]", w["id"])];
        assert_eq!(text(col(row, "customer")), w["customer"].as_str().unwrap());
        assert_eq!(text(col(row, "status")), w["status"].as_str().unwrap());
        assert!((num(col(row, "total")) - w["total"].as_f64().unwrap()).abs() < 1e-9);
        assert_eq!(num(col(row, "updated_at")), w["updated_at"].as_f64().unwrap());
    }
    // items (WITHOUT ROWID, composite key)
    let items = dec.rows("items").unwrap();
    let want = dump["items"].as_array().unwrap();
    assert_eq!(items.len(), want.len(), "items row count");
    for w in want {
        let (_, row) = &items[&format!("[{},{}]", w["order_id"], w["sku"])];
        assert_eq!(num(col(row, "qty")), w["qty"].as_f64().unwrap());
    }
    // attachments: overflow pages, incl. overflow-only retouches
    let att = dec.rows("attachments").unwrap();
    let want = dump["attachments"].as_array().unwrap();
    assert_eq!(att.len(), want.len(), "attachments row count");
    for w in want {
        let (_, row) = &att[&format!("[{}]", w["id"])];
        let Value::Blob(b) = col(row, "data") else { panic!("data not a blob") };
        assert_eq!(b.len() as u64, w["bytes"].as_u64().unwrap());
        let tail: String = b[b.len() - 4..].iter().map(|x| format!("{x:02X}")).collect();
        assert_eq!(tail, w["tail"].as_str().unwrap(), "attachment {} tail (overflow-only retouch)", w["id"]);
    }
    // KV: the keys stored through ctx.storage.put
    let kv = dec.rows("_cf_KV").unwrap();
    let mut got: Vec<String> = kv.values().map(|(_, r)| text(col(r, "k"))).filter(|k| k.starts_with("pref:")).collect();
    got.sort();
    let mut want: Vec<String> = dump["kv"].as_object().unwrap().keys().cloned().collect();
    want.sort();
    assert_eq!(got, want, "KV keys");
    // KV values: V8-decoded JSON must equal what the object returned.
    for (_, r) in kv.values() {
        let k = text(col(r, "k"));
        if let Some(expected) = dump["kv"].get(&k) {
            let decoded = cdcell_core::kv::decode(col(r, "v"));
            let parsed: J = serde_json::from_str(decoded.value_json.as_deref().expect("decodable")).unwrap();
            assert_eq!(&parsed, expected, "KV value {k} ({:?})", decoded.value_type);
        }
    }

    let retouches = meta["ops"]["retouch"].as_u64().unwrap_or(0);
    eprintln!(
        "{}: {} units, orders={} items={} attachments={} kv={}, updates {:?}, retouch ops {}, stats {:?}",
        dir.display(), files.len(), orders.len(), items.len(), att.len(), kv.len(), updates_by_table, retouches, dec.stats
    );
    if retouches > 0 {
        assert!(updates_by_table.get("attachments").copied().unwrap_or(0) > 0, "retouches must surface as attachment updates");
    }
}

#[test]
fn celld_fixtures_match_dump() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut n = 0;
    for e in std::fs::read_dir(&root).unwrap() {
        let p = e.unwrap().path();
        if p.join("dump.json").exists() {
            check_fixture(&p);
            n += 1;
        }
    }
    assert!(n > 0, "no fixtures in {}", root.display());
}
