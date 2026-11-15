//! `cdcell run`: notifications → pipeline → Iceberg (design §3).

use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;

use anyhow::{Context, Result};
use async_nats::jetstream::{self, consumer::pull, AckKind};
use futures::StreamExt;
use tracing::{info, warn};

use crate::keys::{self, ObjKey};
use crate::pipeline::{MsgId, Pipeline, Settings, UnitIn};
use crate::source::Store;

pub struct Config {
    pub nats_url: String,
    pub stream: String,
    pub consumer: String,
    pub source_endpoint: String,
    pub source_bucket: String,
    pub source_prefix: String,
    pub source_key: String,
    pub source_secret: String,
    pub state_endpoint: String,
    pub state_bucket: String,
    pub state_prefix: String,
    pub state_key: String,
    pub state_secret: String,
    pub catalog: String,
    pub warehouse: String,
    pub script: String,
    pub commit_interval: Duration,
    pub gap_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Config {
        let e = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let secs = |k: &str, d: u64| {
            Duration::from_secs(
                std::env::var(k)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(d),
            )
        };
        Config {
            nats_url: e("CDCELL_NATS_URL", "nats://nats:4222"),
            stream: e("CDCELL_STREAM", "CELLD"),
            consumer: e("CDCELL_CONSUMER", "cdcell"),
            source_endpoint: e("CDCELL_SOURCE_ENDPOINT", "http://minio:9000"),
            source_bucket: e("CDCELL_SOURCE_BUCKET", "celld"),
            source_prefix: e("CDCELL_SOURCE_PREFIX", ""),
            source_key: e("CDCELL_SOURCE_ACCESS_KEY", "minioadmin"),
            source_secret: e("CDCELL_SOURCE_SECRET_KEY", "minioadmin"),
            state_endpoint: e("CDCELL_STATE_ENDPOINT", "http://minio-cdc:9000"),
            state_bucket: e("CDCELL_STATE_BUCKET", "warehouse"),
            state_prefix: e("CDCELL_STATE_PREFIX", "_cdcell/"),
            state_key: e("CDCELL_STATE_ACCESS_KEY", "minioadmin"),
            state_secret: e("CDCELL_STATE_SECRET_KEY", "minioadmin"),
            catalog: e("CDCELL_CATALOG", "http://lakekeeper:8181/catalog"),
            warehouse: e("CDCELL_WAREHOUSE", "cdc"),
            script: e("CDCELL_SCRIPT", ""),
            commit_interval: secs("CDCELL_COMMIT_INTERVAL_S", 60),
            gap_timeout: secs("CDCELL_GAP_TIMEOUT_S", 300),
        }
    }
}

/// The Worker script name (design §3): config, else `deploy/current.json`.
async fn script_name(cfg: &Config, src: &Store) -> Result<String> {
    if !cfg.script.is_empty() {
        return Ok(cfg.script.clone());
    }
    let body = src
        .get("deploy/current.json")
        .await?
        .context("deploy/current.json missing; set CDCELL_SCRIPT")?;
    let v: serde_json::Value = serde_json::from_slice(&body)?;
    Ok(v["script_name"]
        .as_str()
        .context("script_name")?
        .to_string())
}

/// Objects named by one notification message (S3 / MinIO `Records`).
fn keys_of(payload: &[u8], prefix: &str) -> Vec<String> {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return Vec::new();
    };
    v["Records"]
        .as_array()
        .map(|rs| {
            rs.iter()
                .filter(|r| {
                    r["eventName"]
                        .as_str()
                        .is_some_and(|n| n.contains("ObjectCreated"))
                })
                .filter_map(|r| r["s3"]["object"]["key"].as_str())
                .map(keys::url_decode)
                .filter_map(|k| k.strip_prefix(prefix).map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

pub async fn run(cfg: Config) -> Result<()> {
    let src = Store::s3(
        &cfg.source_endpoint,
        &cfg.source_bucket,
        &cfg.source_prefix,
        &cfg.source_key,
        &cfg.source_secret,
    )?;
    let state = Store::s3(
        &cfg.state_endpoint,
        &cfg.state_bucket,
        &cfg.state_prefix,
        &cfg.state_key,
        &cfg.state_secret,
    )?;
    let script = script_name(&cfg, &src).await?;
    let sink = crate::iceberg_out::Sink::connect(&cfg.catalog, &cfg.warehouse).await?;
    let mut pipe = Pipeline::new(
        src.clone(),
        state,
        Box::new(sink),
        Settings {
            script: script.clone(),
            gap_timeout: cfg.gap_timeout,
        },
    );

    crate::metrics::serve(
        &std::env::var("CDCELL_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9464".into()),
    )
    .await?;
    pipe.acquire_lease(Duration::from_secs(30)).await?;
    pipe.recover().await?;
    let drop_pct: u64 = std::env::var("CDCELL_TEST_DROP_PCT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let reconcile_every = Duration::from_secs(
        std::env::var("CDCELL_RECONCILE_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300),
    );
    let mut last_reconcile = Instant::now();
    let mut last_renew = Instant::now();

    let nats = async_nats::connect(&cfg.nats_url)
        .await
        .context("connect to NATS")?;
    let js = jetstream::new(nats);
    let stream = js
        .get_stream(&cfg.stream)
        .await
        .context("JetStream stream")?;
    let consumer: jetstream::consumer::Consumer<pull::Config> = stream
        .get_or_create_consumer(
            &cfg.consumer,
            pull::Config {
                durable_name: Some(cfg.consumer.clone()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                max_ack_pending: 50_000,
                ack_wait: (cfg.commit_interval * 3).max(Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await
        .context("JetStream consumer")?;
    info!(%script, stream = %cfg.stream, consumer = %cfg.consumer, "cdcell running");

    let mut held: HashMap<MsgId, jetstream::Message> = HashMap::new();
    let mut next_id: MsgId = 0;
    let mut last_commit = Instant::now();
    loop {
        // 1. intake: up to 500 messages or 1 s
        let mut batch = consumer
            .fetch()
            .max_messages(500)
            .expires(Duration::from_secs(1))
            .messages()
            .await?;
        // Collect the batch, then GET + decode its objects concurrently:
        // bundles are GC'd ~30-40 s after coverage, so intake must keep up
        // (design §3). Units are added in message order afterwards.
        let mut fetched: Vec<(MsgId, Vec<(String, ObjKey)>)> = Vec::new();
        while let Some(msg) = batch.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(%e, "fetch error");
                    break;
                }
            };
            next_id += 1;
            let id = next_id;
            // Test knob: lose N% of notifications.
            let keys = if drop_pct > 0 && (id.wrapping_mul(2654435761) % 100) < drop_pct {
                Vec::new()
            } else {
                keys_of(&msg.payload, &cfg.source_prefix)
                    .into_iter()
                    .filter_map(|k| crate::intake::wanted(&mut pipe, &k).map(|p| (k, p)))
                    .collect()
            };
            fetched.push((id, keys));
            held.insert(id, msg);
        }
        let jobs = fetched
            .iter()
            .flat_map(|(id, keys)| keys.iter().map(move |(k, p)| (*id, k.clone(), p.clone())));
        let mut results: Vec<(MsgId, Vec<UnitIn>)> = futures::stream::iter(jobs)
            .map(|(id, key, parsed)| {
                let src = src.clone();
                async move {
                    let got_at = Instant::now();
                    let units = match src.get(&key).await? {
                        Some(body) => crate::intake::units(&key, parsed, &body, Some(id), got_at),
                        None => Vec::new(),
                    };
                    anyhow::Ok((id, units))
                }
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_>>()?;
        results.sort_by_key(|(id, _)| *id);
        let mut per_msg: HashMap<MsgId, Vec<UnitIn>> = HashMap::new();
        for (id, units) in results {
            per_msg.entry(id).or_default().extend(units);
        }
        for (id, _) in &fetched {
            let units = per_msg.remove(id).unwrap_or_default();
            pipe.track(*id, units.len());
            for u in units {
                pipe.add(u);
            }
        }
        // 2. confirm + apply; periodic tail reconciliation and lease renewal
        if last_reconcile.elapsed() >= reconcile_every {
            pipe.reconcile_bundles().await?;
            last_reconcile = Instant::now();
        }
        if last_renew.elapsed() >= Duration::from_secs(10) {
            pipe.renew_lease().await?;
            last_renew = Instant::now();
        }
        pipe.round().await?;
        // 3. commit
        if last_commit.elapsed() >= cfg.commit_interval {
            pipe.commit().await?;
            last_commit = Instant::now();
        }
        // 4. ack settled messages
        for id in std::mem::take(&mut pipe.ackable) {
            if let Some(m) = held.remove(&id) {
                if let Err(e) = m.ack_with(AckKind::Ack).await {
                    warn!(%e, "ack failed");
                }
            }
        }
        // keep long-held messages alive while they wait (design §3)
        let _ = &held;
    }
}
