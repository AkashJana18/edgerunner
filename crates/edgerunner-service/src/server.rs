use std::{
    collections::BTreeMap,
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Sse, sse::Event},
    routing::{get, post},
};
use edgerunner_adapters::{
    PascalBookAdapter, PascalConfig, TxLineAdapter, TxLineConfig, TxLineProofClient,
};
use edgerunner_core::{
    DislocationTaker, Engine, EngineSnapshot, FeedStatus, JournalRecord, JournalWriter, OrderMode,
};
use futures_util::Stream;
use serde::Serialize;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

use crate::config::BackendConfig;
use crate::simulation;

type TradingEngine = Engine<DislocationTaker>;

struct Runtime {
    engine: TradingEngine,
    running: bool,
    killed: bool,
    feed_status: BTreeMap<String, FeedStatus>,
}

impl Runtime {
    fn snapshot(&self) -> EngineSnapshot {
        self.engine
            .snapshot(self.running, self.killed, self.feed_status.clone())
    }
}

#[derive(Clone)]
struct AppState {
    runtime: Arc<RwLock<Runtime>>,
    snapshots: broadcast::Sender<EngineSnapshot>,
    control_token: Arc<String>,
    config: BackendConfig,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    mode: OrderMode,
}

#[derive(Serialize)]
struct Readiness {
    ready: bool,
    reason: &'static str,
}

pub struct LiveFeedConfig {
    pub market: String,
    pub pascal_symbol: String,
}

struct ProofRequest {
    order_id: uuid::Uuid,
    message_id: String,
    proof_ts: i64,
}

pub async fn serve(
    bind: &str,
    journal_path: PathBuf,
    live: Option<LiveFeedConfig>,
    config: BackendConfig,
) -> Result<()> {
    let address: SocketAddr = bind.parse().context("invalid bind address")?;
    let configured_control_token = std::env::var("EDGERUNNER_CONTROL_TOKEN").ok();
    let control_token = resolve_control_token(address, configured_control_token)?;
    let mut feed_status = BTreeMap::new();
    let initial_status = if live.is_some() {
        FeedStatus::Connecting
    } else {
        FeedStatus::Live
    };
    feed_status.insert("txline".into(), initial_status);
    feed_status.insert("pascal".into(), initial_status);
    let runtime = Arc::new(RwLock::new(Runtime {
        engine: Engine::new(
            DislocationTaker::new(config.strategy.clone()),
            config.engine(),
        ),
        running: true,
        killed: false,
        feed_status,
    }));
    let (snapshots, _) = broadcast::channel(128);
    let (journal_tx, journal_rx) = mpsc::channel(4096);
    spawn_journal(journal_path, journal_rx);
    journal_tx
        .send(JournalRecord::Run {
            schema: 1,
            run_id: runtime.read().await.engine.run_id,
            started_at: chrono::Utc::now(),
        })
        .await
        .context("journal worker stopped during startup")?;
    if let Some(live) = live {
        spawn_live_feeds(runtime.clone(), snapshots.clone(), journal_tx, live)?;
    } else {
        spawn_simulator(runtime.clone(), snapshots.clone(), journal_tx);
    }

    let state = AppState {
        runtime,
        snapshots,
        control_token: Arc::new(control_token),
        config,
    };
    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/ready", get(ready))
        .route("/api/config", get(effective_config))
        .route("/api/snapshot", get(snapshot))
        .route("/api/events", get(events))
        .route("/api/metrics", get(metrics))
        .route("/api/kill", post(kill))
        .route("/api/resume", post(resume))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .fallback_service(
            ServeDir::new("web/dist").not_found_service(ServeFile::new("web/dist/index.html")),
        );

    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "EdgeRunner service listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Health> {
    Json(Health {
        status: "ok",
        mode: OrderMode::Paper,
    })
}

async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let runtime = state.runtime.read().await;
    let feeds_live = runtime
        .feed_status
        .values()
        .all(|status| *status == FeedStatus::Live);
    let (status, body) = if !runtime.running {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Readiness {
                ready: false,
                reason: "engine stopped",
            },
        )
    } else if runtime.killed {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Readiness {
                ready: false,
                reason: "kill switch active",
            },
        )
    } else if !feeds_live {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Readiness {
                ready: false,
                reason: "feed unavailable",
            },
        )
    } else {
        (
            StatusCode::OK,
            Readiness {
                ready: true,
                reason: "ready",
            },
        )
    };
    (status, Json(body))
}

async fn effective_config(State(state): State<AppState>) -> Json<BackendConfig> {
    Json(state.config)
}

async fn snapshot(State(state): State<AppState>) -> Json<EngineSnapshot> {
    Json(state.runtime.read().await.snapshot())
}

async fn events(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = BroadcastStream::new(state.snapshots.subscribe()).filter_map(|result| {
        result.ok().and_then(|snapshot| {
            serde_json::to_string(&snapshot)
                .ok()
                .map(|json| Ok(Event::default().event("snapshot").data(json)))
        })
    });
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

async fn kill(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED;
    }
    let mut runtime = state.runtime.write().await;
    runtime.killed = true;
    runtime.engine.set_killed(true);
    let snapshot = runtime.snapshot();
    drop(runtime);
    let _ = state.snapshots.send(snapshot);
    StatusCode::NO_CONTENT
}

async fn resume(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED;
    }
    let mut runtime = state.runtime.write().await;
    runtime.killed = false;
    runtime.engine.set_killed(false);
    let snapshot = runtime.snapshot();
    drop(runtime);
    let _ = state.snapshots.send(snapshot);
    StatusCode::NO_CONTENT
}

fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    headers
        .get("x-api-token")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == state.control_token.as_str())
}

async fn metrics(State(state): State<AppState>) -> String {
    let runtime = state.runtime.read().await;
    let latency = runtime.engine.latency_snapshot();
    format!(
        concat!(
            "# TYPE edgerunner_events_total counter\n",
            "edgerunner_events_total {}\n",
            "# TYPE edgerunner_orders_rejected_total counter\n",
            "edgerunner_orders_rejected_total {}\n",
            "# TYPE edgerunner_events_ignored_total counter\n",
            "edgerunner_events_ignored_total {}\n",
            "# TYPE edgerunner_decision_p99_microseconds gauge\n",
            "edgerunner_decision_p99_microseconds {}\n",
            "# TYPE edgerunner_position_contracts gauge\n",
            "edgerunner_position_contracts {}\n",
            "# TYPE edgerunner_pnl_micros gauge\n",
            "edgerunner_pnl_micros {}\n",
            "# TYPE edgerunner_kill_switch gauge\n",
            "edgerunner_kill_switch {}\n"
        ),
        runtime.engine.processed_events,
        runtime.engine.rejected_orders,
        runtime.engine.ignored_events,
        latency.p99_us,
        runtime.engine.state.position,
        runtime.engine.state.pnl_micros,
        u8::from(runtime.killed),
    )
}

fn spawn_journal(path: PathBuf, mut receiver: mpsc::Receiver<JournalRecord>) {
    tokio::task::spawn_blocking(move || {
        let mut writer = match JournalWriter::create(path) {
            Ok(writer) => writer,
            Err(error) => {
                tracing::error!(%error, "failed to open journal");
                return;
            }
        };
        while let Some(record) = receiver.blocking_recv() {
            if let Err(error) = writer.write(&record) {
                tracing::error!(%error, "failed to write journal record");
            }
        }
    });
}

fn spawn_simulator(
    runtime: Arc<RwLock<Runtime>>,
    snapshots: broadcast::Sender<EngineSnapshot>,
    journal: mpsc::Sender<JournalRecord>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(350));
        let mut tick = 0_u64;
        loop {
            interval.tick().await;
            tick += 1;
            let now = epoch_ns();
            for event in simulation::events(tick, now) {
                let _ = journal
                    .send(JournalRecord::Event {
                        schema: 1,
                        event: event.clone(),
                    })
                    .await;
                let (records, snapshot) = {
                    let mut guard = runtime.write().await;
                    let mut records = Vec::new();
                    for output in guard.engine.process(event) {
                        records.push(JournalRecord::Decision {
                            schema: 1,
                            decision: output.decision,
                        });
                        if let Some(fill) = output.fill {
                            records.push(JournalRecord::Fill { schema: 1, fill });
                        }
                    }
                    (records, guard.snapshot())
                };
                for record in records {
                    let _ = journal.send(record).await;
                }
                let _ = snapshots.send(snapshot);
            }
        }
    });
}

fn spawn_live_feeds(
    runtime: Arc<RwLock<Runtime>>,
    snapshots: broadcast::Sender<EngineSnapshot>,
    journal: mpsc::Sender<JournalRecord>,
    live: LiveFeedConfig,
) -> Result<()> {
    if live.pascal_symbol.is_empty() {
        anyhow::bail!("--pascal-symbol is required with --live-feeds");
    }
    let guest_jwt = std::env::var("TXLINE_GUEST_JWT").context("TXLINE_GUEST_JWT is required")?;
    let api_token = std::env::var("TXLINE_API_TOKEN").context("TXLINE_API_TOKEN is required")?;
    let txline_origin =
        std::env::var("TXLINE_ORIGIN").unwrap_or_else(|_| "https://txline.txodds.com".into());
    let pascal_ws =
        std::env::var("PASCAL_WS_URL").unwrap_or_else(|_| "wss://ws.pascal.trade".into());
    let (sender, mut receiver) = mpsc::channel(4096);

    let txline_config = TxLineConfig {
        origin: txline_origin,
        guest_jwt,
        api_token,
        market: live.market.clone(),
    };
    let proof_client = TxLineProofClient::new(&txline_config)?;
    let (proof_sender, proof_receiver) = mpsc::channel(512);
    spawn_proof_worker(proof_client, journal.clone(), proof_receiver);
    let txline_sender = sender.clone();
    let txline_runtime = runtime.clone();
    let txline_snapshots = snapshots.clone();
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            set_feed_status(
                &txline_runtime,
                &txline_snapshots,
                "txline",
                FeedStatus::Connecting,
            )
            .await;
            let result = match TxLineAdapter::new(txline_config.clone()) {
                Ok(adapter) => adapter.stream_odds(txline_sender.clone()).await,
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                tracing::warn!(%error, ?backoff, "TxLINE stream reconnecting");
            }
            set_feed_status(
                &txline_runtime,
                &txline_snapshots,
                "txline",
                FeedStatus::Disconnected,
            )
            .await;
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    });

    let pascal_config = PascalConfig {
        ws_url: pascal_ws,
        symbol: live.pascal_symbol,
        market: live.market,
    };
    let pascal_runtime = runtime.clone();
    let pascal_snapshots = snapshots.clone();
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            set_feed_status(
                &pascal_runtime,
                &pascal_snapshots,
                "pascal",
                FeedStatus::Connecting,
            )
            .await;
            if let Err(error) = PascalBookAdapter::new(pascal_config.clone())
                .stream_book(sender.clone())
                .await
            {
                tracing::warn!(%error, ?backoff, "Pascal stream reconnecting");
            }
            set_feed_status(
                &pascal_runtime,
                &pascal_snapshots,
                "pascal",
                FeedStatus::Disconnected,
            )
            .await;
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    });

    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let feed = match event.event {
                edgerunner_core::MarketEvent::FairValue { .. } => Some("txline"),
                edgerunner_core::MarketEvent::Book { .. } => Some("pascal"),
                _ => None,
            };
            let _ = journal
                .send(JournalRecord::Event {
                    schema: 1,
                    event: event.clone(),
                })
                .await;
            let (records, proof_requests, snapshot) = {
                let mut guard = runtime.write().await;
                if let Some(feed) = feed {
                    guard.feed_status.insert(feed.into(), FeedStatus::Live);
                }
                let mut records = Vec::new();
                let mut proof_requests = Vec::new();
                for output in guard.engine.process(event) {
                    if output.decision.action == "submitted"
                        && let Some(intent) = output.decision.intent.as_ref()
                        && let (Some(message_id), Some(proof_ts)) =
                            (&intent.source_message_id, intent.source_proof_ts)
                    {
                        proof_requests.push(ProofRequest {
                            order_id: intent.id,
                            message_id: message_id.clone(),
                            proof_ts,
                        });
                    }
                    records.push(JournalRecord::Decision {
                        schema: 1,
                        decision: output.decision,
                    });
                    if let Some(fill) = output.fill {
                        records.push(JournalRecord::Fill { schema: 1, fill });
                    }
                }
                (records, proof_requests, guard.snapshot())
            };
            for record in records {
                let _ = journal.send(record).await;
            }
            for request in proof_requests {
                let _ = proof_sender.send(request).await;
            }
            let _ = snapshots.send(snapshot);
        }
    });
    Ok(())
}

fn spawn_proof_worker(
    client: TxLineProofClient,
    journal: mpsc::Sender<JournalRecord>,
    mut receiver: mpsc::Receiver<ProofRequest>,
) {
    tokio::spawn(async move {
        const RETRY_DELAYS: [Duration; 6] = [
            Duration::from_secs(0),
            Duration::from_secs(15),
            Duration::from_secs(30),
            Duration::from_secs(60),
            Duration::from_secs(120),
            Duration::from_secs(240),
        ];
        while let Some(request) = receiver.recv().await {
            let mut last_error = None;
            for delay in RETRY_DELAYS {
                tokio::time::sleep(delay).await;
                match client
                    .fetch_odds_proof(&request.message_id, request.proof_ts)
                    .await
                {
                    Ok(proof) => {
                        let _ = journal
                            .send(JournalRecord::Proof {
                                schema: 1,
                                order_id: request.order_id,
                                message_id: request.message_id.clone(),
                                proof_ts: request.proof_ts,
                                fetched_at: chrono::Utc::now(),
                                proof,
                            })
                            .await;
                        last_error = None;
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            if let Some(error) = last_error {
                tracing::warn!(
                    %error,
                    order_id = %request.order_id,
                    message_id = %request.message_id,
                    "TxLINE proof unavailable after retries"
                );
            }
        }
    });
}

async fn set_feed_status(
    runtime: &Arc<RwLock<Runtime>>,
    snapshots: &broadcast::Sender<EngineSnapshot>,
    feed: &str,
    status: FeedStatus,
) {
    let snapshot = {
        let mut guard = runtime.write().await;
        guard.feed_status.insert(feed.to_owned(), status);
        guard.snapshot()
    };
    let _ = snapshots.send(snapshot);
}

fn epoch_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn resolve_control_token(address: SocketAddr, configured: Option<String>) -> Result<String> {
    if !address.ip().is_loopback() && configured.is_none() {
        anyhow::bail!("EDGERUNNER_CONTROL_TOKEN is required for a non-loopback bind address");
    }
    Ok(configured.unwrap_or_else(|| "local-demo".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_loopback_bind_requires_explicit_control_token() {
        let public: SocketAddr = "0.0.0.0:8080".parse().unwrap();
        let local: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        assert!(resolve_control_token(public, None).is_err());
        assert_eq!(
            resolve_control_token(public, Some("secret".into())).unwrap(),
            "secret"
        );
        assert_eq!(resolve_control_token(local, None).unwrap(), "local-demo");
    }
}
