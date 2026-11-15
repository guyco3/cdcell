//! Prometheus metrics (design §9), served on :9464.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Default)]
pub struct Metrics {
    pub events: AtomicU64,
    pub units: AtomicU64,
    pub commits: AtomicU64,
    pub commit_micros: AtomicU64,
    pub phantoms_dropped: AtomicU64,
    pub duplicates_dropped: AtomicU64,
    pub not_found: AtomicU64,
    pub decode_errors: AtomicU64,
    pub resyncs: AtomicU64,
    pub rebuilds: AtomicU64,
    pub reconciled_bundles: AtomicU64,
    pub confirm_reads: AtomicU64,
    pub s3_get: AtomicU64,
    pub s3_list: AtomicU64,
    pub s3_put: AtomicU64,
    // gauges (f64 bits)
    pub open_gaps: AtomicU64,
    pub parked_cells: AtomicU64,
    pub age_max: AtomicU64,
    pub age_p99: AtomicU64,
    pub parked_age_max: AtomicU64,
}

pub static M: std::sync::LazyLock<Metrics> = std::sync::LazyLock::new(Metrics::default);

pub fn inc(c: &AtomicU64) {
    c.fetch_add(1, Relaxed);
}

pub fn add(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Relaxed);
}

pub fn set(g: &AtomicU64, v: f64) {
    g.store(v.to_bits(), Relaxed);
}

fn g(v: &AtomicU64) -> f64 {
    f64::from_bits(v.load(Relaxed))
}

pub fn render() -> String {
    let m = &*M;
    let c = |v: &AtomicU64| v.load(Relaxed);
    let mut s = String::new();
    let mut counter = |name: &str, help: &str, v: u64| {
        s.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}\n"
        ));
    };
    counter(
        "cdcell_events_total",
        "Change events emitted.",
        c(&m.events),
    );
    counter("cdcell_units_total", "LTX units received.", c(&m.units));
    counter(
        "cdcell_commits_total",
        "Iceberg batches committed.",
        c(&m.commits),
    );
    counter(
        "cdcell_phantoms_dropped_total",
        "Units dropped as phantoms or superseded by a newer epoch.",
        c(&m.phantoms_dropped),
    );
    counter(
        "cdcell_duplicates_dropped_total",
        "Units dropped as already applied.",
        c(&m.duplicates_dropped),
    );
    counter(
        "cdcell_not_found_total",
        "Notified objects gone before the GET (404).",
        c(&m.not_found),
    );
    counter(
        "cdcell_decode_errors_total",
        "Objects that failed to decode.",
        c(&m.decode_errors),
    );
    counter(
        "cdcell_resyncs_total",
        "Cells resynchronized from a rebuilt image.",
        c(&m.resyncs),
    );
    counter(
        "cdcell_rebuilds_total",
        "Cells rebuilt or resynced from the celld bucket after a restart.",
        c(&m.rebuilds),
    );
    counter(
        "cdcell_reconciled_bundles_total",
        "Bundles fetched by reconciliation.",
        c(&m.reconciled_bundles),
    );
    counter(
        "cdcell_confirm_reads_total",
        "own.json / lease reads for confirmation.",
        c(&m.confirm_reads),
    );
    s.push_str("# HELP cdcell_s3_requests_total Object storage requests.\n# TYPE cdcell_s3_requests_total counter\n");
    for (op, v) in [("get", &m.s3_get), ("list", &m.s3_list), ("put", &m.s3_put)] {
        s.push_str(&format!(
            "cdcell_s3_requests_total{{op=\"{op}\"}} {}\n",
            c(v)
        ));
    }
    s.push_str(&format!(
        "# HELP cdcell_commit_seconds Commit latency.\n# TYPE cdcell_commit_seconds summary\ncdcell_commit_seconds_sum {}\ncdcell_commit_seconds_count {}\n",
        c(&m.commit_micros) as f64 / 1e6,
        c(&m.commits)
    ));
    for (name, help, v) in [
        (
            "cdcell_open_gaps",
            "Cells blocked on a missing unit.",
            g(&m.open_gaps),
        ),
        (
            "cdcell_parked_cells",
            "Cells with parked suspect units.",
            g(&m.parked_cells),
        ),
        (
            "cdcell_watermark_age_seconds_max",
            "Max age of the oldest uncommitted change per cell.",
            g(&m.age_max),
        ),
        (
            "cdcell_watermark_age_seconds_p99",
            "p99 over cells of the oldest uncommitted change age.",
            g(&m.age_p99),
        ),
        (
            "cdcell_parked_age_seconds_max",
            "Age of the oldest parked suspect unit (undecided until a later epoch opens).",
            g(&m.parked_age_max),
        ),
    ] {
        s.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n"
        ));
    }
    s
}

/// Minimal HTTP server: every request gets the metrics page.
pub async fn serve(addr: &str) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                continue;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                let body = render();
                let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    Ok(())
}
