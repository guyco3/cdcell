//! M1 gate, part 1: incremental decoding is exact.
//!
//! A seeded workload mutates a real SQLite database. After every unit (one or
//! more committed transactions), the pages that changed are fed to the
//! decoder. A model built only from the decoder's events must equal SQLite's
//! own view of every table (`SELECT * FROM t`), and every update or delete
//! must carry a `before` image equal to the model's current row.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use cdcell_core::schema::Generated;
use cdcell_core::value::key_json;
use cdcell_core::{Decoder, Op, SchemaChange, Value};
use rusqlite::{params, Connection};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn f(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn n(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % (hi - lo + 1) as u64) as i64
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
    fn pick<T: Copy>(&mut self, xs: &[T]) -> T {
        xs[(self.next() % xs.len() as u64) as usize]
    }
}

type Model = HashMap<String, HashMap<String, BTreeMap<String, Value>>>;

fn sqlite_value(v: rusqlite::types::ValueRef<'_>) -> Value {
    use rusqlite::types::ValueRef as V;
    match v {
        V::Null => Value::Null,
        V::Integer(i) => Value::Integer(i),
        V::Real(f) => Value::Real(f),
        V::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        V::Blob(b) => Value::Blob(b.to_vec()),
    }
}

/// SQLite's own view of every table, keyed the way the decoder keys rows.
fn oracle(conn: &Connection, dec: &Decoder) -> Model {
    let mut out = Model::new();
    for info in dec.tables() {
        if info.name.starts_with("sqlite_") {
            continue;
        }
        let q = info.name.replace('"', "\"\"");
        let sql = if info.without_rowid { format!("SELECT * FROM \"{q}\"") } else { format!("SELECT rowid AS __rowid, * FROM \"{q}\"") };
        let mut stmt = conn.prepare(&sql).unwrap();
        let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = stmt.query([]).unwrap();
        let table = out.entry(info.name.clone()).or_default();
        while let Some(r) = rows.next().unwrap() {
            let mut m: BTreeMap<String, Value> = BTreeMap::new();
            let mut rowid = None;
            for (i, n) in names.iter().enumerate() {
                let v = sqlite_value(r.get_ref(i).unwrap());
                if n == "__rowid" {
                    if let Value::Integer(x) = v {
                        rowid = Some(x);
                    }
                } else {
                    m.insert(n.clone(), v);
                }
            }
            for c in &info.columns {
                if c.generated == Some(Generated::Virtual) {
                    m.remove(&c.name); // not stored; the decoder does not compute it
                }
            }
            let key = if info.pk.is_empty() {
                vec![Value::Integer(rowid.unwrap())]
            } else {
                let k: Vec<Value> = info.pk.iter().map(|&c| m[&info.columns[c].name].clone()).collect();
                if !info.without_rowid && k.iter().any(Value::is_null) {
                    vec![Value::Text(format!("$rowid:{}", rowid.unwrap()))]
                } else {
                    k
                }
            };
            table.insert(key_json(&key), m);
        }
    }
    out
}

fn check(model: &Model, conn: &Connection, dec: &Decoder, step: usize) {
    let truth = oracle(conn, dec);
    for (table, rows) in &truth {
        let info = dec.table_info(table).unwrap();
        let empty = HashMap::new();
        let got = model.get(table).unwrap_or(&empty);
        assert_eq!(rows.len(), got.len(), "step {step} table {table}: row count");
        for (k, want) in rows {
            let have = got.get(k).unwrap_or_else(|| panic!("step {step} table {table}: missing key {k}"));
            for (col, v) in want {
                let default = info.columns.iter().find(|c| &c.name == col).map(|c| c.default.clone()).unwrap_or(Value::Null);
                let hv = have.get(col).unwrap_or(&default);
                assert_eq!(hv, v, "step {step} table {table} key {k} column {col}");
            }
        }
    }
    for table in model.keys() {
        assert!(truth.contains_key(table) || model[table].is_empty(), "step {step}: model has dropped table {table}");
    }
}

fn run(seed: u64, auto_vacuum: bool, batch: usize, steps: usize) -> (cdcell_core::decoder::Stats, [u64; 3]) {
    let dir = PathBuf::from(std::env::temp_dir()).join(format!("cdcell-prop-{}-{seed}-{auto_vacuum}-{batch}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("db.sqlite");
    let conn = Connection::open(&path).unwrap();
    if auto_vacuum {
        conn.execute_batch("PRAGMA auto_vacuum=FULL").unwrap();
    }
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE _cf_KV (scope TEXT, k TEXT, v TEXT, PRIMARY KEY(scope, k));
         CREATE TABLE t1 (id INTEGER PRIMARY KEY, a TEXT, b BLOB);
         CREATE INDEX t1a ON t1(a);
         CREATE TABLE t2 (a TEXT, b INT, v BLOB, n INT, PRIMARY KEY (a, b)) WITHOUT ROWID;
         CREATE TABLE t3 (a, b);
         CREATE TABLE t4 (id TEXT PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x * 2) STORED,
                          z INT GENERATED ALWAYS AS (x + 1) VIRTUAL);",
    )
    .unwrap();
    let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);
    let mut dec = Decoder::new();
    let mut prev: Vec<u8> = Vec::new();
    let mut model = Model::new();
    let mut counts = [0u64; 3];
    let mut extra: Vec<String> = Vec::new();
    let mut t3b = "b".to_string();
    let mut added = 0;
    let mut t1_extra: Option<String> = None;
    let mut pending = 0;
    let mut uniq = 0u64;
    for step in 0..steps {
        conn.execute_batch("BEGIN").unwrap();
        for _ in 0..rng.n(1, 20) {
            let op = rng.f();
            if op < 0.20 {
                let v: String = std::iter::repeat('x').take(rng.n(1, 600) as usize).collect();
                conn.execute("INSERT OR REPLACE INTO _cf_KV VALUES ('s', ?1, ?2)", params![format!("key{:04}", rng.n(0, 2000)), v]).unwrap();
            } else if op < 0.35 {
                let size = rng.pick(&[10usize, 100, 1000, 5000, 20000]);
                conn.execute("INSERT OR REPLACE INTO t1 (id, a, b) VALUES (?1, ?2, ?3)", params![rng.n(1, 1500), format!("a{}", rng.n(0, 99)), rng.bytes(size)]).unwrap();
            } else if op < 0.43 {
                // same-size overwrite differing only in the overflow tail
                let row: Option<(i64, Vec<u8>)> = conn.query_row("SELECT id, b FROM t1 WHERE length(b) > 4500 ORDER BY random() LIMIT 1", [], |r| Ok((r.get(0)?, r.get(1)?))).ok();
                if let Some((id, mut b)) = row {
                    let last = b.len() - 1;
                    b[last] ^= 0xff;
                    conn.execute("UPDATE t1 SET b = ?1 WHERE id = ?2", params![b, id]).unwrap();
                }
            } else if op < 0.55 {
                let size = rng.pick(&[5usize, 200, 3000, 9000]);
                conn.execute("INSERT OR REPLACE INTO t2 (a, b, v, n) VALUES (?1, ?2, ?3, ?4)", params![format!("k{:03}", rng.n(0, 300)), rng.n(0, 4), rng.bytes(size), rng.n(0, 9)]).unwrap();
            } else if op < 0.63 {
                let v: rusqlite::types::Value = match rng.n(0, 4) {
                    0 => rusqlite::types::Value::Integer(rng.n(-1000, 1000)),
                    1 => rusqlite::types::Value::Real(rng.f() * 100.0),
                    2 => rusqlite::types::Value::Text(format!("t{}", rng.n(0, 50))),
                    3 => { let n = rng.n(0, 50) as usize; rusqlite::types::Value::Blob(rng.bytes(n)) }
                    _ => rusqlite::types::Value::Null,
                };
                if rng.f() < 0.5 {
                    conn.execute(&format!("INSERT INTO t3 (a, {t3b}) VALUES (?1, ?2)"), params![rng.n(0, 9), v]).unwrap();
                } else {
                    conn.execute(&format!("UPDATE t3 SET {t3b} = ?1 WHERE rowid = (SELECT rowid FROM t3 ORDER BY random() LIMIT 1)"), params![v]).unwrap();
                }
            } else if op < 0.70 {
                let id: Option<String> = if rng.f() < 0.1 { None } else { Some(format!("id{}", rng.n(0, 200))) };
                conn.execute("INSERT OR REPLACE INTO t4 (id, x) VALUES (?1, ?2)", params![id, rng.n(0, 1000)]).unwrap();
            } else if op < 0.76 {
                let lo = rng.n(1, 1500);
                conn.execute("DELETE FROM t1 WHERE id BETWEEN ?1 AND ?2", params![lo, lo + rng.n(0, 60)]).unwrap();
            } else if op < 0.81 {
                let lo = rng.n(0, 300);
                conn.execute("DELETE FROM t2 WHERE a BETWEEN ?1 AND ?2", params![format!("k{lo:03}"), format!("k{:03}", lo + 20)]).unwrap();
            } else if op < 0.85 {
                let lo = rng.n(0, 2000);
                conn.execute("DELETE FROM _cf_KV WHERE k BETWEEN ?1 AND ?2", params![format!("key{lo:04}"), format!("key{:04}", lo + 80)]).unwrap();
            } else if op < 0.88 {
                conn.execute("UPDATE OR REPLACE t1 SET id = id + 5000 WHERE id = ?1", params![rng.n(1, 1500)]).unwrap();
            } else if op < 0.91 {
                conn.execute("DELETE FROM t3 WHERE a = ?1", params![rng.n(0, 9)]).unwrap();
            } else if op < 0.93 && extra.len() < 3 {
                uniq += 1;
                let name = format!("x{uniq}");
                conn.execute_batch(&format!("CREATE TABLE {name} AS SELECT * FROM t1 LIMIT {}", rng.n(0, 200))).unwrap();
                extra.push(name);
            } else if op < 0.945 && !extra.is_empty() {
                conn.execute_batch(&format!("DROP TABLE {}", extra.remove(0))).unwrap();
            } else if op < 0.955 && added < 4 {
                uniq += 1;
                conn.execute_batch(&format!("ALTER TABLE t2 ADD COLUMN c{uniq} INT DEFAULT 7")).unwrap();
                added += 1;
            } else if op < 0.962 {
                uniq += 1;
                let new = format!("b{uniq}");
                conn.execute_batch(&format!("ALTER TABLE t3 RENAME COLUMN {t3b} TO {new}")).unwrap();
                t3b = new;
            } else if op < 0.97 {
                match t1_extra.take() {
                    None => {
                        uniq += 1;
                        let c = format!("e{uniq}");
                        conn.execute_batch(&format!("ALTER TABLE t1 ADD COLUMN {c} TEXT DEFAULT 'dflt'")).unwrap();
                        t1_extra = Some(c);
                    }
                    Some(c) => conn.execute_batch(&format!("ALTER TABLE t1 DROP COLUMN {c}")).unwrap(),
                }
            }
        }
        conn.execute_batch("COMMIT").unwrap();
        if rng.f() < 0.01 {
            conn.execute_batch("VACUUM").unwrap();
        }
        pending += 1;
        if pending < batch {
            continue;
        }
        pending = 0;

        let cur = std::fs::read(&path).unwrap();
        let ps = 4096;
        let changed: Vec<(u32, Vec<u8>)> = cur
            .chunks(ps)
            .enumerate()
            .filter(|(i, p)| prev.get(i * ps..(i + 1) * ps) != Some(*p))
            .map(|(i, p)| (i as u32 + 1, p.to_vec()))
            .collect();
        let commit = (cur.len() / ps) as u32;
        let changes = dec.apply(changed, commit).unwrap_or_else(|e| panic!("seed {seed} step {step}: {e}"));
        for s in &changes.schema {
            match s {
                SchemaChange::DropTable { table, .. } => {
                    model.remove(table);
                }
                SchemaChange::AlterTable { table, renamed: Some((old, new)), .. } => {
                    for row in model.entry(table.clone()).or_default().values_mut() {
                        if let Some(v) = row.remove(old) {
                            row.insert(new.clone(), v);
                        }
                    }
                }
                _ => {}
            }
        }
        for c in changes.rows {
            let t = model.entry(c.table.clone()).or_default();
            let k = c.key_json();
            let named = |r: &Option<Vec<(String, Value)>>| -> BTreeMap<String, Value> { r.clone().unwrap().into_iter().collect() };
            match c.op {
                Op::Insert => {
                    assert!(!t.contains_key(&k), "seed {seed} step {step}: insert of existing {}/{k}", c.table);
                    t.insert(k, named(&c.after));
                    counts[0] += 1;
                }
                Op::Update | Op::Delete => {
                    let cur = t.get(&k).unwrap_or_else(|| panic!("seed {seed} step {step}: {:?} of unknown {}/{k}", c.op, c.table));
                    for (col, v) in named(&c.before) {
                        if let Some(m) = cur.get(&col) {
                            assert_eq!(m, &v, "seed {seed} step {step}: before image {}/{k}.{col}", c.table);
                        }
                    }
                    if c.op == Op::Update {
                        t.insert(k, named(&c.after));
                        counts[1] += 1;
                    } else {
                        t.remove(&k);
                        counts[2] += 1;
                    }
                }
            }
        }
        check(&model, &conn, &dec, step);
        prev = cur;
    }
    let _ = std::fs::remove_dir_all(&dir);
    (dec.stats.clone(), counts)
}

#[test]
fn exact_across_seeds_and_batch_sizes() {
    for (seed, auto_vacuum, batch) in [(1, false, 1), (2, false, 3), (3, true, 1), (4, false, 7), (5, true, 3), (6, false, 1)] {
        let (stats, [ins, upd, del]) = run(seed, auto_vacuum, batch, 250);
        eprintln!(
            "seed={seed} autovac={auto_vacuum} batch={batch}: events c={ins} u={upd} d={del}; pages in={} unchanged={} decoded={} rewalks={} schema_reads={}",
            stats.pages_in, stats.pages_unchanged, stats.pages_decoded, stats.rewalks, stats.schema_reads
        );
        assert!(ins > 0 && upd > 0 && del > 0);
    }
}
