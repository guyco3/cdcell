//! cdcell: CDC from celld Durable Objects to Iceberg.

mod emit;
mod iceberg_out;
mod keys;
mod pipeline;
mod run;
mod source;

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use cdcell_core::schema::is_internal;
use cdcell_core::Decoder;

fn now_us() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64
}

/// Streams a captured fixture (one cell's L0 files, design M1 fixture
/// format) through decode → emit → Iceberg, one snapshot per table.
async fn selftest_fixture(dir: PathBuf, uri: &str, warehouse: &str, script: &str) -> Result<()> {
    let meta: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.join("dump.json"))?)?;
    let cell = meta["cell"].as_str().context("fixture cell")?.to_string();
    let class = cell.split(':').next().unwrap().to_string();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?.map(|e| e.unwrap().path()).filter(|p| p.extension().is_some_and(|x| x == "ltx")).collect();
    files.sort();

    let mut dec = Decoder::new();
    let mut by_table: HashMap<String, Vec<iceberg_out::OutRow>> = HashMap::new();
    let mut infos = HashMap::new();
    for f in &files {
        let unit = cdcell_core::ltx::decode_file(&std::fs::read(f)?)?;
        let epoch: u64 = f.file_name().unwrap().to_string_lossy()[1..5].parse()?;
        let changes = dec.apply(unit.pages, unit.commit)?;
        for t in dec.tables() {
            infos.insert(t.name.clone(), t.clone());
        }
        let rows: Vec<_> = changes.rows.into_iter().filter(|c| !is_internal(&c.table)).collect();
        let ctx = emit::Ctx {
            script, class: &class, cell: &cell, epoch, txid: unit.max_txid, capture_ms: unit.timestamp_ms,
            now_us: now_us(), ce_type_prefix: "dev", schema_version: 1, kind: emit::Kind::Streamed,
        };
        for (table, row) in emit::rows(&ctx, &|t| dec.table_info(t).cloned(), &rows) {
            by_table.entry(table).or_default().push(row);
        }
    }
    let sink = iceberg_out::Sink::connect(uri, warehouse).await?;
    let ns = vec!["cdcell".to_string(), iceberg_out::ident(script), iceberg_out::ident(&class)];
    for (table, rows) in &by_table {
        let info = &infos[table];
        let ident = sink
            .ensure_table(&ns, &iceberg_out::ident(table), iceberg_out::change_schema(info)?, HashMap::from([("cdcell.original-name".into(), table.clone())]))
            .await?;
        sink.append(&ident, rows, HashMap::from([("cdcell.batch".into(), "1".into())])).await?;
        println!("{}: appended {} events", ident, rows.len());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let flag = |name: &str, default: &str| -> String {
        args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned().unwrap_or_else(|| default.to_string())
    };
    match args.get(1).map(String::as_str) {
        Some("selftest-fixture") => {
            let dir = PathBuf::from(args.get(2).context("fixture dir")?);
            selftest_fixture(dir, &flag("--catalog", "http://lakekeeper:8181/catalog"), &flag("--warehouse", "cdc"), &flag("--script", "shop")).await
        }
        Some("run") => {
            tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())).init();
            run::run(run::Config::from_env()).await
        }
        _ => bail!("usage: cdcell run | cdcell selftest-fixture DIR [--catalog URI] [--warehouse W] [--script S]"),
    }
}
