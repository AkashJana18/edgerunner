mod config;
mod mapping;
mod server;

use std::{path::PathBuf, time::Instant};

use anyhow::Result;
use clap::{Parser, Subcommand};
use edgerunner_core::{DislocationTaker, Engine, replay, replay_with_limit};
use tracing_subscriber::EnvFilter;

use crate::config::BackendConfig;

#[derive(Parser)]
#[command(
    name = "edgerunner",
    about = "Deterministic low-latency prediction market engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        #[arg(long, default_value = "data/runs/latest.jsonl")]
        journal: PathBuf,
        #[arg(long)]
        live_feeds: bool,
        #[arg(long)]
        market: Option<String>,
        #[arg(long)]
        pascal_symbol: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    Replay {
        #[arg(long)]
        journal: PathBuf,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    Bench {
        /// A journal recorded by the live TxLINE and Pascal adapters.
        #[arg(long)]
        journal: PathBuf,
        /// Limit the number of recorded events processed. `--iterations` remains an alias.
        #[arg(long, visible_alias = "iterations")]
        max_events: Option<u64>,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    Probe {
        #[arg(long = "url", required = true)]
        urls: Vec<String>,
        #[arg(long, default_value_t = 5)]
        samples: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Local credentials are kept out of version control in `.env`; process variables still win.
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "edgerunner=info".into()),
        )
        .init();

    match Cli::parse().command.unwrap_or(Command::Serve {
        bind: "127.0.0.1:8080".into(),
        journal: "data/runs/latest.jsonl".into(),
        live_feeds: true,
        market: None,
        pascal_symbol: None,
        config: None,
    }) {
        Command::Serve {
            bind,
            journal,
            live_feeds,
            market,
            pascal_symbol,
            config,
        } => {
            let backend_config = BackendConfig::load(config.as_deref())?;
            let live = live_feeds.then_some(server::LiveFeedConfig {
                market,
                pascal_symbol,
            });
            server::serve(&bind, journal, live, backend_config).await
        }
        Command::Replay { journal, config } => {
            run_replay(journal, BackendConfig::load(config.as_deref())?)
        }
        Command::Bench {
            journal,
            max_events,
            config,
        } => run_bench(journal, max_events, BackendConfig::load(config.as_deref())?),
        Command::Probe { urls, samples } => run_probe(urls, samples).await,
    }
}

fn engine(config: &BackendConfig) -> Engine<DislocationTaker> {
    Engine::new(
        DislocationTaker::new(config.strategy.clone()),
        config.engine(),
    )
}

fn run_replay(journal: PathBuf, config: BackendConfig) -> Result<()> {
    let report = replay(journal, &mut engine(&config))
        .map_err(|error| anyhow::anyhow!("replay failed: {error}"))?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn run_bench(journal: PathBuf, max_events: Option<u64>, config: BackendConfig) -> Result<()> {
    let mut engine = engine(&config);
    let start = Instant::now();
    let report = replay_with_limit(&journal, &mut engine, max_events)
        .map_err(|error| anyhow::anyhow!("benchmark failed: {error}"))?;
    let elapsed = start.elapsed();
    println!(
        "{}",
        serde_json::json!({
            "journal": journal,
            "source": "recorded_txline_and_pascal",
            "events": report.events,
            "elapsed_ms": elapsed.as_millis(),
            "events_per_second": report.events as f64 / elapsed.as_secs_f64(),
            "replay": report,
            "decision_latency": engine.latency_snapshot()
        })
    );
    Ok(())
}

async fn run_probe(urls: Vec<String>, samples: usize) -> Result<()> {
    let client = reqwest::Client::builder().tcp_nodelay(true).build()?;
    for url in urls {
        let mut values = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            let status = client.get(&url).send().await?.status();
            values.push(start.elapsed().as_micros() as u64);
            tracing::debug!(%status, %url, "probe response");
        }
        values.sort_unstable();
        println!(
            "{}",
            serde_json::json!({
                "url": url,
                "samples": samples,
                "p50_us": values[values.len() / 2],
                "min_us": values[0],
                "max_us": values[values.len() - 1]
            })
        );
    }
    Ok(())
}
