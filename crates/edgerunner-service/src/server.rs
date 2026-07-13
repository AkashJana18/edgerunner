use std::{
    collections::BTreeMap, convert::Infallible, net::SocketAddr, path::PathBuf, sync::Arc,
    time::Duration,
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
    DislocationTaker, Engine, EngineSnapshot, FeedStatus, JournalRecord, JournalWriter,
    MarketDataSource, OrderMode,
};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, RwLock, broadcast, mpsc},
    task::JoinHandle,
};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};

use crate::config::BackendConfig;
type TradingEngine = Engine<DislocationTaker>;

struct Runtime {
    engine: TradingEngine,
    running: bool,
    killed: bool,
    feed_mode: FeedMode,
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
    journal: mpsc::Sender<JournalRecord>,
    feeds: Arc<Mutex<FeedController>>,
    control_token: Arc<String>,
    config: BackendConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FeedMode {
    Inactive,
    Live,
}

struct FeedController {
    mode: FeedMode,
    live: Option<ConfiguredLiveFeed>,
    workers: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
struct ConfiguredLiveFeed {
    market: String,
    pascal_symbol: String,
    fixture_id: u64,
    txline_origin: String,
    api_token: String,
    pascal_ws: String,
}

impl ConfiguredLiveFeed {
    fn from_environment(market: Option<String>, pascal_symbol: Option<String>) -> Result<Self> {
        let market = market
            .or_else(|| optional_env("TXLINE_MARKET"))
            .context("TXLINE_MARKET is required for live feeds")?;
        let pascal_symbol = pascal_symbol
            .or_else(|| optional_env("PASCAL_SYMBOL"))
            .context("PASCAL_SYMBOL is required for live feeds")?;
        let fixture_id = required_env("TXLINE_FIXTURE_ID")?
            .parse()
            .context("TXLINE_FIXTURE_ID must be an unsigned integer")?;
        let api_token = required_env("TXLINE_API_TOKEN")?;
        let config = Self {
            market,
            pascal_symbol,
            fixture_id,
            txline_origin: std::env::var("TXLINE_ORIGIN")
                .unwrap_or_else(|_| "https://txline-dev.txodds.com".into()),
            api_token,
            pascal_ws: std::env::var("PASCAL_WS_URL")
                .unwrap_or_else(|_| "wss://data.pascal.trade/ws".into()),
        };
        config.validate()?;
        Ok(config)
    }

    fn optional_from_environment() -> Result<Option<Self>> {
        let keys = [
            "TXLINE_API_TOKEN",
            "TXLINE_MARKET",
            "PASCAL_SYMBOL",
            "TXLINE_FIXTURE_ID",
        ];
        let configured = keys
            .iter()
            .filter(|key| optional_env(key).is_some())
            .count();
        if configured == 0 {
            return Ok(None);
        }
        Self::from_environment(None, None).map(Some)
    }

    fn validate(&self) -> Result<()> {
        if self.pascal_symbol.trim().is_empty() {
            anyhow::bail!("PASCAL_SYMBOL cannot be empty");
        }
        if !self.pascal_ws.starts_with("ws://") && !self.pascal_ws.starts_with("wss://") {
            anyhow::bail!("PASCAL_WS_URL must use ws:// or wss://");
        }
        TxLineAdapter::new(TxLineConfig {
            origin: self.txline_origin.clone(),
            api_token: self.api_token.clone(),
            market: self.market.clone(),
            fixture_id: self.fixture_id,
        })?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct SetFeedMode {
    mode: FeedMode,
}

#[derive(Serialize)]
struct FeedModeState {
    mode: FeedMode,
    live_available: bool,
}

#[derive(Serialize)]
struct ApiError {
    error: String,
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
    pub market: Option<String>,
    pub pascal_symbol: Option<String>,
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
    let live = match live {
        Some(config) => Some(ConfiguredLiveFeed::from_environment(
            config.market,
            config.pascal_symbol,
        )?),
        None => ConfiguredLiveFeed::optional_from_environment()?,
    };
    let initial_mode = if live.is_some() {
        FeedMode::Live
    } else {
        FeedMode::Inactive
    };
    let runtime = Arc::new(RwLock::new(Runtime {
        engine: Engine::new(
            DislocationTaker::new(config.strategy.clone()),
            config.engine(),
        ),
        running: true,
        killed: false,
        feed_mode: initial_mode,
        feed_status: feed_status_for(initial_mode),
    }));
    let (snapshots, _) = broadcast::channel(128);
    let (journal_tx, journal_rx) = mpsc::channel(4096);
    spawn_journal(journal_path, journal_rx);
    journal_tx
        .send(JournalRecord::Run {
            schema: 2,
            run_id: runtime.read().await.engine.run_id,
            started_at: chrono::Utc::now(),
        })
        .await
        .context("journal worker stopped during startup")?;
    let workers = if let Some(live) = live.clone() {
        spawn_live_feeds(runtime.clone(), snapshots.clone(), journal_tx.clone(), live)?
    } else {
        Vec::new()
    };

    let state = AppState {
        runtime,
        snapshots,
        journal: journal_tx,
        feeds: Arc::new(Mutex::new(FeedController {
            mode: initial_mode,
            live,
            workers,
        })),
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
        .route("/api/feed-mode", get(feed_mode).post(set_feed_mode))
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
    } else if runtime.feed_mode == FeedMode::Inactive {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Readiness {
                ready: false,
                reason: "live feeds inactive",
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

async fn feed_mode(State(state): State<AppState>) -> Json<FeedModeState> {
    let controller = state.feeds.lock().await;
    Json(FeedModeState {
        mode: controller.mode,
        live_available: controller.live.is_some(),
    })
}

async fn set_feed_mode(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SetFeedMode>,
) -> Result<Json<FeedModeState>, (StatusCode, Json<ApiError>)> {
    if !authorized(&state, &headers) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiError {
                error: "control token rejected".into(),
            }),
        ));
    }
    switch_feed_mode(&state, request.mode)
        .await
        .map(Json)
        .map_err(|error| {
            (
                StatusCode::CONFLICT,
                Json(ApiError {
                    error: error.to_string(),
                }),
            )
        })
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

fn spawn_live_feeds(
    runtime: Arc<RwLock<Runtime>>,
    snapshots: broadcast::Sender<EngineSnapshot>,
    journal: mpsc::Sender<JournalRecord>,
    live: ConfiguredLiveFeed,
) -> Result<Vec<JoinHandle<()>>> {
    live.validate()?;
    let (sender, mut receiver) = mpsc::channel(4096);

    let txline_config = TxLineConfig {
        origin: live.txline_origin,
        api_token: live.api_token,
        market: live.market.clone(),
        fixture_id: live.fixture_id,
    };
    let proof_client = TxLineProofClient::new(&txline_config)?;
    let (proof_sender, proof_receiver) = mpsc::channel(512);
    let proof_worker = spawn_proof_worker(proof_client, journal.clone(), proof_receiver);
    let txline_sender = sender.clone();
    let txline_runtime = runtime.clone();
    let txline_snapshots = snapshots.clone();
    let txline_worker = tokio::spawn(async move {
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
        ws_url: live.pascal_ws,
        symbol: live.pascal_symbol,
        market: live.market,
    };
    let pascal_runtime = runtime.clone();
    let pascal_snapshots = snapshots.clone();
    let pascal_worker = tokio::spawn(async move {
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

    let event_worker = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let (feed, source) = match &event.event {
                edgerunner_core::MarketEvent::FairValue { .. } => {
                    ("txline", MarketDataSource::Txline)
                }
                edgerunner_core::MarketEvent::Book { .. } => ("pascal", MarketDataSource::Pascal),
                _ => {
                    tracing::warn!("dropping market event without a configured live source");
                    continue;
                }
            };
            let _ = journal
                .send(JournalRecord::Event {
                    schema: 2,
                    source,
                    event: event.clone(),
                })
                .await;
            let (records, proof_requests, snapshot) = {
                let mut guard = runtime.write().await;
                guard.feed_status.insert(feed.into(), FeedStatus::Live);
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
                        schema: 2,
                        decision: output.decision,
                    });
                    if let Some(fill) = output.fill {
                        records.push(JournalRecord::Fill { schema: 2, fill });
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
    Ok(vec![
        proof_worker,
        txline_worker,
        pascal_worker,
        event_worker,
    ])
}

fn spawn_proof_worker(
    client: TxLineProofClient,
    journal: mpsc::Sender<JournalRecord>,
    mut receiver: mpsc::Receiver<ProofRequest>,
) -> JoinHandle<()> {
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
                                schema: 2,
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
    })
}

async fn switch_feed_mode(state: &AppState, mode: FeedMode) -> Result<FeedModeState> {
    let mut controller = state.feeds.lock().await;
    if controller.mode == mode {
        return Ok(FeedModeState {
            mode,
            live_available: controller.live.is_some(),
        });
    }

    let live = match mode {
        FeedMode::Inactive => None,
        FeedMode::Live => Some(
            controller
                .live
                .clone()
                .context("live feeds are not configured on this server")?,
        ),
    };
    if let Some(live) = &live {
        live.validate()?;
    }

    for worker in controller.workers.drain(..) {
        worker.abort();
    }
    let (run_record, snapshot) = reset_runtime(&state.runtime, &state.config, mode).await;
    state
        .journal
        .send(run_record)
        .await
        .context("journal worker stopped during feed switch")?;
    let _ = state.snapshots.send(snapshot);

    controller.workers = match live {
        Some(live) => spawn_live_feeds(
            state.runtime.clone(),
            state.snapshots.clone(),
            state.journal.clone(),
            live,
        )?,
        None => Vec::new(),
    };
    controller.mode = mode;
    Ok(FeedModeState {
        mode,
        live_available: controller.live.is_some(),
    })
}

async fn reset_runtime(
    runtime: &Arc<RwLock<Runtime>>,
    config: &BackendConfig,
    feed_mode: FeedMode,
) -> (JournalRecord, EngineSnapshot) {
    let mut runtime = runtime.write().await;
    let killed = runtime.killed;
    runtime.engine = Engine::new(
        DislocationTaker::new(config.strategy.clone()),
        config.engine(),
    );
    runtime.engine.set_killed(killed);
    runtime.feed_mode = feed_mode;
    runtime.feed_status = feed_status_for(feed_mode);
    let run_id = runtime.engine.run_id;
    let snapshot = runtime.snapshot();
    (
        JournalRecord::Run {
            schema: 2,
            run_id,
            started_at: chrono::Utc::now(),
        },
        snapshot,
    )
}

fn feed_status_for(mode: FeedMode) -> BTreeMap<String, FeedStatus> {
    let status = match mode {
        FeedMode::Inactive => FeedStatus::Disconnected,
        FeedMode::Live => FeedStatus::Connecting,
    };
    BTreeMap::from([("txline".to_owned(), status), ("pascal".to_owned(), status)])
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn required_env(key: &str) -> Result<String> {
    optional_env(key).with_context(|| format!("{key} is required for live feeds"))
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

    #[test]
    fn source_statuses_do_not_present_inactive_feeds_as_live() {
        let inactive = feed_status_for(FeedMode::Inactive);
        let live = feed_status_for(FeedMode::Live);
        assert!(
            inactive
                .values()
                .all(|status| *status == FeedStatus::Disconnected)
        );
        assert!(
            live.values()
                .all(|status| *status == FeedStatus::Connecting)
        );
    }
}
