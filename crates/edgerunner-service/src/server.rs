use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs::File,
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
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
    DislocationTaker, Engine, EngineSnapshot, EventEnvelope, FeedStatus, JournalRecord,
    JournalWriter, MarketDataSource, MarketEvent, OrderMode,
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
use crate::mapping::{DiscoveryConfig, ResolvedLiveFeed};
type TradingEngine = Engine<DislocationTaker>;

struct Runtime {
    engine: TradingEngine,
    running: bool,
    killed: bool,
    feed_mode: FeedMode,
    feed_status: BTreeMap<String, FeedStatus>,
    replay: ReplayRuntime,
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
    replay_events: Arc<Vec<RecordedEvent>>,
    replay_journal: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FeedMode {
    Inactive,
    Live,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Environment {
    Devnet,
    Mainnet,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum RunMode {
    Live,
    Replay,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReplayStatus {
    Paused,
    Playing,
    Complete,
    Unavailable,
}

#[derive(Clone, Debug, Serialize)]
struct ReplayRuntime {
    status: ReplayStatus,
    event_index: usize,
    total_events: usize,
    speed: f64,
    journal: String,
}

#[derive(Clone, Debug)]
struct RecordedEvent {
    event: EventEnvelope,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ReplayCommandRequest {
    Play,
    Pause,
    Reset,
    SetSpeed { speed: f64 },
    Seek { event_index: usize },
}

#[derive(Clone, Copy, Debug)]
enum ReplayCommand {
    Play,
    Pause,
    Reset,
    SetSpeed(f64),
    Seek(usize),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionMode {
    Simulated,
}

#[derive(Clone, Debug, Serialize)]
struct MarketDisplay {
    event: String,
    contract: String,
    period: String,
    starts_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct SessionState {
    environment: Environment,
    run_mode: RunMode,
    execution: ExecutionMode,
    live_available: bool,
    mapping_status: MappingStatus,
    replay: ReplayRuntime,
    market: Option<MarketDisplay>,
}

#[derive(Clone, Debug, Deserialize)]
struct SetSessionRequest {
    environment: Option<Environment>,
    run_mode: Option<RunMode>,
}

struct FeedController {
    mode: FeedMode,
    environment: Environment,
    run_mode: RunMode,
    live: Option<ResolvedLiveFeed>,
    discovery: MappingStatus,
    workers: Vec<JoinHandle<()>>,
    replay_tx: Option<mpsc::Sender<ReplayCommand>>,
}

#[derive(Deserialize)]
struct SetFeedMode {
    mode: FeedMode,
}

#[derive(Serialize)]
struct FeedModeState {
    mode: FeedMode,
    live_available: bool,
    mapping_status: MappingStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MappingStatus {
    Unavailable,
    Discovering,
    Ready,
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
    let replay_journal = journal_path.display().to_string();
    let replay_events = Arc::new(load_replay_events(&journal_path).unwrap_or_else(|error| {
        tracing::warn!(%error, path = %journal_path.display(), "replay journal unavailable");
        Vec::new()
    }));
    let resolver = match live {
        Some(config) => Some(DiscoveryConfig::from_environment(
            config.market,
            config.pascal_symbol,
        )?),
        None => DiscoveryConfig::optional_from_environment(None, None)?,
    };
    let initial_mode = FeedMode::Inactive;
    let runtime = Arc::new(RwLock::new(Runtime {
        engine: Engine::new(
            DislocationTaker::new(config.strategy.clone()),
            config.engine(),
        ),
        running: true,
        killed: false,
        feed_mode: initial_mode,
        feed_status: feed_status_for(initial_mode),
        replay: ReplayRuntime {
            status: if replay_events.is_empty() {
                ReplayStatus::Unavailable
            } else {
                ReplayStatus::Paused
            },
            event_index: 0,
            total_events: replay_events.len(),
            speed: 1.0,
            journal: replay_journal.clone(),
        },
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
    let state = AppState {
        runtime,
        snapshots,
        journal: journal_tx,
        feeds: Arc::new(Mutex::new(FeedController {
            mode: initial_mode,
            environment: Environment::Devnet,
            run_mode: RunMode::Live,
            live: None,
            discovery: if resolver.is_some() {
                MappingStatus::Discovering
            } else {
                MappingStatus::Unavailable
            },
            workers: Vec::new(),
            replay_tx: None,
        })),
        control_token: Arc::new(control_token),
        config,
        replay_events,
        replay_journal,
    };
    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/ready", get(ready))
        .route("/api/config", get(effective_config))
        .route("/api/snapshot", get(snapshot))
        .route("/api/events", get(events))
        .route("/api/metrics", get(metrics))
        .route("/api/session", get(session).post(set_session))
        .route("/api/replay", post(replay_command))
        .route("/api/feed-mode", get(feed_mode).post(set_feed_mode))
        .route("/api/kill", post(kill))
        .route("/api/resume", post(resume))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http())
        .fallback_service(
            ServeDir::new("web/dist").not_found_service(ServeFile::new("web/dist/index.html")),
        );

    if let Some(resolver) = resolver {
        spawn_mapping_discovery(state.clone(), resolver);
    }

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
        mapping_status: controller.discovery,
    })
}

async fn session(State(state): State<AppState>) -> Json<SessionState> {
    let controller = state.feeds.lock().await;
    let runtime = state.runtime.read().await;
    Json(session_state(&controller, &runtime))
}

async fn set_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SetSessionRequest>,
) -> Result<Json<SessionState>, (StatusCode, Json<ApiError>)> {
    if !authorized(&state, &headers) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiError {
                error: "control token rejected".into(),
            }),
        ));
    }
    let current = {
        let controller = state.feeds.lock().await;
        (controller.environment, controller.run_mode)
    };
    let environment = request.environment.unwrap_or(current.0);
    let run_mode = request.run_mode.unwrap_or(current.1);
    switch_session(&state, environment, run_mode)
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

async fn replay_command(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ReplayCommandRequest>,
) -> Result<Json<SessionState>, (StatusCode, Json<ApiError>)> {
    if !authorized(&state, &headers) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiError {
                error: "control token rejected".into(),
            }),
        ));
    }
    let command = match request {
        ReplayCommandRequest::Play => ReplayCommand::Play,
        ReplayCommandRequest::Pause => ReplayCommand::Pause,
        ReplayCommandRequest::Reset => ReplayCommand::Reset,
        ReplayCommandRequest::SetSpeed { speed } => {
            if !speed.is_finite() || !(0.25..=20.0).contains(&speed) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ApiError {
                        error: "replay speed must be between 0.25x and 20x".into(),
                    }),
                ));
            }
            ReplayCommand::SetSpeed(speed)
        }
        ReplayCommandRequest::Seek { event_index } => ReplayCommand::Seek(event_index),
    };
    let sender = {
        let controller = state.feeds.lock().await;
        if controller.run_mode != RunMode::Replay {
            return Err((
                StatusCode::CONFLICT,
                Json(ApiError {
                    error: "replay controls are available only in replay mode".into(),
                }),
            ));
        }
        controller.replay_tx.clone()
    };
    let Some(sender) = sender else {
        return Err((
            StatusCode::CONFLICT,
            Json(ApiError {
                error: "replay is unavailable for the selected journal".into(),
            }),
        ));
    };
    sender.send(command).await.map_err(|_| {
        (
            StatusCode::CONFLICT,
            Json(ApiError {
                error: "replay worker is no longer running".into(),
            }),
        )
    })?;
    let controller = state.feeds.lock().await;
    let runtime = state.runtime.read().await;
    Ok(Json(session_state(&controller, &runtime)))
}

fn session_state(controller: &FeedController, runtime: &Runtime) -> SessionState {
    let live_available = controller.environment == Environment::Devnet
        && controller.live.is_some()
        && controller.discovery == MappingStatus::Ready;
    let market = controller.live.as_ref().and_then(|live| {
        let active_market = &runtime.engine.state.market;
        (active_market.is_empty() || active_market == &live.mapping.market).then(|| MarketDisplay {
            event: live.event_label.clone(),
            contract: live.contract_label.clone(),
            period: live.market_period.clone(),
            starts_at_ms: live.expected_event_start_time_ms,
        })
    });
    SessionState {
        environment: controller.environment,
        run_mode: controller.run_mode,
        execution: ExecutionMode::Simulated,
        live_available,
        mapping_status: if controller.environment == Environment::Mainnet {
            MappingStatus::Unavailable
        } else {
            controller.discovery
        },
        replay: runtime.replay.clone(),
        market,
    }
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
        let mut writer = match JournalWriter::open(path) {
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
    live: ResolvedLiveFeed,
) -> Result<Vec<JoinHandle<()>>> {
    let (sender, mut receiver) = mpsc::channel(4096);

    let txline_config = TxLineConfig {
        origin: live.txline_origin.clone(),
        api_token: live.api_token.clone(),
        market: live.mapping.market.clone(),
        fixture_id: live.mapping.fixture_id,
        selection: live.mapping.txline_selection.clone(),
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
        symbol: live.mapping.pascal_symbol,
        market: live.mapping.market,
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
            process_market_event(
                runtime.clone(),
                snapshots.clone(),
                journal.clone(),
                Some(proof_sender.clone()),
                event,
                true,
            )
            .await;
        }
    });
    Ok(vec![
        proof_worker,
        txline_worker,
        pascal_worker,
        event_worker,
    ])
}

async fn process_market_event(
    runtime: Arc<RwLock<Runtime>>,
    snapshots: broadcast::Sender<EngineSnapshot>,
    journal: mpsc::Sender<JournalRecord>,
    proof_sender: Option<mpsc::Sender<ProofRequest>>,
    event: EventEnvelope,
    record_event: bool,
) {
    let (feed, source) = match &event.event {
        edgerunner_core::MarketEvent::FairValue { .. } => ("txline", MarketDataSource::Txline),
        edgerunner_core::MarketEvent::Book { .. } => ("pascal", MarketDataSource::Pascal),
        _ => {
            tracing::warn!("dropping market event without a configured source");
            return;
        }
    };
    if record_event {
        let _ = journal
            .send(JournalRecord::Event {
                schema: 2,
                source,
                event: event.clone(),
            })
            .await;
    }
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
            if let Some(trade) = output.trade {
                records.push(JournalRecord::Trade { schema: 2, trade });
            }
        }
        (records, proof_requests, guard.snapshot())
    };
    for record in records {
        let _ = journal.send(record).await;
    }
    if let Some(proof_sender) = proof_sender {
        for request in proof_requests {
            let _ = proof_sender.send(request).await;
        }
    }
    let _ = snapshots.send(snapshot);
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

fn load_replay_events(path: &PathBuf) -> Result<Vec<RecordedEvent>> {
    if !path.exists() {
        anyhow::bail!("replay journal does not exist")
    }
    let file =
        File::open(path).with_context(|| format!("open replay journal {}", path.display()))?;
    let mut events = Vec::new();
    for (line_number, line) in BufReader::new(file).lines().enumerate() {
        let record: JournalRecord = serde_json::from_str(
            &line.with_context(|| format!("read replay journal line {}", line_number + 1))?,
        )
        .with_context(|| format!("parse replay journal line {}", line_number + 1))?;
        if let JournalRecord::Event { source, event, .. } = record {
            let valid = match (&source, &event.event) {
                (
                    MarketDataSource::Txline,
                    MarketEvent::FairValue {
                        message_id: Some(message_id),
                        proof_ts: Some(_),
                        ..
                    },
                ) if !message_id.trim().is_empty() => true,
                (MarketDataSource::Pascal, MarketEvent::Book { .. }) => true,
                _ => false,
            };
            if valid {
                events.push(RecordedEvent { event });
            }
        }
    }
    Ok(events)
}

async fn switch_session(
    state: &AppState,
    environment: Environment,
    run_mode: RunMode,
) -> Result<SessionState> {
    let mut controller = state.feeds.lock().await;
    for worker in controller.workers.drain(..) {
        worker.abort();
    }
    controller.replay_tx = None;
    controller.environment = environment;
    controller.run_mode = run_mode;
    controller.mode = FeedMode::Inactive;

    match run_mode {
        RunMode::Replay => {
            let (run_record, snapshot) =
                reset_runtime(&state.runtime, &state.config, FeedMode::Inactive).await;
            let _ = state.journal.send(run_record).await;
            let _ = state.snapshots.send(snapshot);
            let (sender, worker) = spawn_replay_worker(
                state.runtime.clone(),
                state.snapshots.clone(),
                state.journal.clone(),
                state.config.clone(),
                state.replay_events.clone(),
                state.replay_journal.clone(),
            );
            controller.replay_tx = Some(sender);
            controller.workers.push(worker);
        }
        RunMode::Live => {
            let feed_mode = if environment == Environment::Devnet && controller.live.is_some() {
                FeedMode::Live
            } else {
                FeedMode::Inactive
            };
            let (run_record, snapshot) =
                reset_runtime(&state.runtime, &state.config, feed_mode).await;
            let _ = state.journal.send(run_record).await;
            let _ = state.snapshots.send(snapshot);
            if feed_mode == FeedMode::Live
                && let Some(live) = controller.live.clone()
            {
                controller.workers = spawn_live_feeds(
                    state.runtime.clone(),
                    state.snapshots.clone(),
                    state.journal.clone(),
                    live,
                )?;
                controller.mode = FeedMode::Live;
            }
        }
    }

    let runtime = state.runtime.read().await;
    Ok(session_state(&controller, &runtime))
}

fn spawn_replay_worker(
    runtime: Arc<RwLock<Runtime>>,
    snapshots: broadcast::Sender<EngineSnapshot>,
    journal: mpsc::Sender<JournalRecord>,
    config: BackendConfig,
    events: Arc<Vec<RecordedEvent>>,
    replay_journal: String,
) -> (mpsc::Sender<ReplayCommand>, JoinHandle<()>) {
    let (sender, mut receiver) = mpsc::channel(32);
    let command_sender = sender.clone();
    let worker = tokio::spawn(async move {
        let mut index = 0_usize;
        let mut speed = 1.0_f64;
        let mut playing = false;
        let mut previous_time_ns = None;
        update_replay_state(
            &runtime,
            &snapshots,
            if events.is_empty() {
                ReplayStatus::Unavailable
            } else {
                ReplayStatus::Paused
            },
            index,
            events.len(),
            speed,
            &replay_journal,
        )
        .await;

        loop {
            if !playing {
                let Some(command) = receiver.recv().await else {
                    return;
                };
                if handle_replay_command(
                    command,
                    &runtime,
                    &snapshots,
                    &journal,
                    &config,
                    &events,
                    &replay_journal,
                    &mut index,
                    &mut speed,
                    &mut previous_time_ns,
                    &mut playing,
                )
                .await
                {
                    return;
                }
                continue;
            }

            if index >= events.len() {
                playing = false;
                update_replay_state(
                    &runtime,
                    &snapshots,
                    ReplayStatus::Complete,
                    index,
                    events.len(),
                    speed,
                    &replay_journal,
                )
                .await;
                continue;
            }

            let delay = replay_delay(
                previous_time_ns,
                events[index].event.received_time_ns,
                speed,
            );
            tokio::select! {
                command = receiver.recv() => {
                    let Some(command) = command else { return; };
                    if handle_replay_command(
                        command,
                        &runtime,
                        &snapshots,
                        &journal,
                        &config,
                        &events,
                        &replay_journal,
                        &mut index,
                        &mut speed,
                        &mut previous_time_ns,
                        &mut playing,
                    ).await { return; }
                }
                _ = tokio::time::sleep(delay) => {
                    let recorded = events[index].clone();
                    previous_time_ns = Some(recorded.event.received_time_ns);
                    process_market_event(
                        runtime.clone(),
                        snapshots.clone(),
                        journal.clone(),
                        None,
                        recorded.event,
                        false,
                    ).await;
                    index = index.saturating_add(1);
                    update_replay_state(
                        &runtime,
                        &snapshots,
                        if index >= events.len() { ReplayStatus::Complete } else { ReplayStatus::Playing },
                        index,
                        events.len(),
                        speed,
                        &replay_journal,
                    ).await;
                    if index >= events.len() { playing = false; }
                }
            }
        }
    });
    (command_sender, worker)
}

#[allow(clippy::too_many_arguments)]
async fn handle_replay_command(
    command: ReplayCommand,
    runtime: &Arc<RwLock<Runtime>>,
    snapshots: &broadcast::Sender<EngineSnapshot>,
    journal: &mpsc::Sender<JournalRecord>,
    config: &BackendConfig,
    events: &[RecordedEvent],
    replay_journal: &str,
    index: &mut usize,
    speed: &mut f64,
    previous_time_ns: &mut Option<u64>,
    playing: &mut bool,
) -> bool {
    match command {
        ReplayCommand::Play => {
            if events.is_empty() {
                *playing = false;
                update_replay_state(
                    runtime,
                    snapshots,
                    ReplayStatus::Unavailable,
                    *index,
                    events.len(),
                    *speed,
                    replay_journal,
                )
                .await;
                return false;
            }
            if *index >= events.len() {
                let (run_record, snapshot) =
                    reset_runtime(runtime, config, FeedMode::Inactive).await;
                let _ = journal.send(run_record).await;
                let _ = snapshots.send(snapshot);
                *index = 0;
                *previous_time_ns = None;
            }
            *playing = true;
            update_replay_state(
                runtime,
                snapshots,
                ReplayStatus::Playing,
                *index,
                events.len(),
                *speed,
                replay_journal,
            )
            .await;
        }
        ReplayCommand::Pause => {
            *playing = false;
            update_replay_state(
                runtime,
                snapshots,
                ReplayStatus::Paused,
                *index,
                events.len(),
                *speed,
                replay_journal,
            )
            .await;
        }
        ReplayCommand::Reset => {
            let (run_record, snapshot) = reset_runtime(runtime, config, FeedMode::Inactive).await;
            let _ = journal.send(run_record).await;
            let _ = snapshots.send(snapshot);
            *index = 0;
            *previous_time_ns = None;
            *playing = false;
            update_replay_state(
                runtime,
                snapshots,
                if events.is_empty() {
                    ReplayStatus::Unavailable
                } else {
                    ReplayStatus::Paused
                },
                *index,
                events.len(),
                *speed,
                replay_journal,
            )
            .await;
        }
        ReplayCommand::SetSpeed(next_speed) => {
            *speed = next_speed.clamp(0.25, 20.0);
            update_replay_state(
                runtime,
                snapshots,
                if *playing {
                    ReplayStatus::Playing
                } else {
                    ReplayStatus::Paused
                },
                *index,
                events.len(),
                *speed,
                replay_journal,
            )
            .await;
        }
        ReplayCommand::Seek(target) => {
            let target = target.min(events.len());
            let (run_record, snapshot) = reset_runtime(runtime, config, FeedMode::Inactive).await;
            let _ = journal.send(run_record).await;
            let _ = snapshots.send(snapshot);
            *index = 0;
            *previous_time_ns = None;
            *playing = false;
            for recorded in events.iter().take(target).cloned() {
                *previous_time_ns = Some(recorded.event.received_time_ns);
                process_market_event(
                    runtime.clone(),
                    snapshots.clone(),
                    journal.clone(),
                    None,
                    recorded.event,
                    false,
                )
                .await;
                *index = (*index).saturating_add(1);
            }
            update_replay_state(
                runtime,
                snapshots,
                if *index >= events.len() {
                    ReplayStatus::Complete
                } else {
                    ReplayStatus::Paused
                },
                *index,
                events.len(),
                *speed,
                replay_journal,
            )
            .await;
        }
    }
    false
}

fn replay_delay(previous_time_ns: Option<u64>, current_time_ns: u64, speed: f64) -> Duration {
    let Some(previous_time_ns) = previous_time_ns else {
        return Duration::ZERO;
    };
    let delta_ns = current_time_ns.saturating_sub(previous_time_ns);
    let scaled_ns = (delta_ns as f64 / speed).min(2_000_000_000.0) as u64;
    Duration::from_nanos(scaled_ns.max(20_000_000))
}

async fn update_replay_state(
    runtime: &Arc<RwLock<Runtime>>,
    snapshots: &broadcast::Sender<EngineSnapshot>,
    status: ReplayStatus,
    event_index: usize,
    total_events: usize,
    speed: f64,
    journal: &str,
) {
    let snapshot = {
        let mut guard = runtime.write().await;
        guard.replay = ReplayRuntime {
            status,
            event_index,
            total_events,
            speed,
            journal: journal.to_owned(),
        };
        guard.snapshot()
    };
    let _ = snapshots.send(snapshot);
}

async fn switch_feed_mode(state: &AppState, mode: FeedMode) -> Result<FeedModeState> {
    let mut controller = state.feeds.lock().await;
    if controller.mode == mode {
        return Ok(FeedModeState {
            mode,
            live_available: controller.environment == Environment::Devnet
                && controller.live.is_some(),
            mapping_status: controller.discovery,
        });
    }

    if mode == FeedMode::Live && controller.environment == Environment::Mainnet {
        anyhow::bail!("mainnet live feeds are not configured")
    }

    let live = match mode {
        FeedMode::Inactive => None,
        FeedMode::Live => {
            let reason = match controller.discovery {
                MappingStatus::Discovering => "market mapping discovery is still in progress",
                MappingStatus::Unavailable => "live feeds require TXLINE_API_TOKEN",
                MappingStatus::Ready => "discovered live feed is unavailable",
            };
            Some(controller.live.clone().context(reason)?)
        }
    };
    for worker in controller.workers.drain(..) {
        worker.abort();
    }
    controller.replay_tx = None;
    controller.run_mode = RunMode::Live;
    let (run_record, snapshot) = reset_runtime(&state.runtime, &state.config, mode).await;
    state
        .journal
        .send(run_record)
        .await
        .context("journal worker stopped during feed switch")?;
    if let Some(live) = &live {
        state
            .journal
            .send(JournalRecord::Mapping {
                schema: 2,
                mapping: live.mapping.clone(),
            })
            .await
            .context("journal worker stopped during feed switch")?;
    }
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
        live_available: controller.environment == Environment::Devnet && controller.live.is_some(),
        mapping_status: controller.discovery,
    })
}

fn spawn_mapping_discovery(state: AppState, resolver: DiscoveryConfig) {
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(5);
        loop {
            match resolver.resolve().await {
                Ok(live) => {
                    let mut controller = state.feeds.lock().await;
                    if controller.live.is_some() {
                        return;
                    }
                    if state
                        .journal
                        .send(JournalRecord::Mapping {
                            schema: 2,
                            mapping: live.mapping.clone(),
                        })
                        .await
                        .is_err()
                    {
                        tracing::error!(
                            "journal worker stopped while saving discovered market mapping"
                        );
                        return;
                    }
                    controller.live = Some(live.clone());
                    controller.discovery = MappingStatus::Ready;
                    if controller.environment != Environment::Devnet
                        || controller.run_mode != RunMode::Live
                    {
                        tracing::info!(
                            environment = ?controller.environment,
                            run_mode = ?controller.run_mode,
                            "discovered live mapping is waiting for a compatible session"
                        );
                        return;
                    }
                    let (run_record, snapshot) =
                        reset_runtime(&state.runtime, &state.config, FeedMode::Live).await;
                    if state.journal.send(run_record).await.is_err() {
                        tracing::error!("journal worker stopped while starting discovered feed");
                        return;
                    }
                    match spawn_live_feeds(
                        state.runtime.clone(),
                        state.snapshots.clone(),
                        state.journal.clone(),
                        live,
                    ) {
                        Ok(workers) => {
                            controller.workers = workers;
                            controller.mode = FeedMode::Live;
                            let _ = state.snapshots.send(snapshot);
                            tracing::info!(
                                "discovered and activated a TxLINE-to-Pascal market mapping"
                            );
                            return;
                        }
                        Err(error) => {
                            tracing::warn!(%error, "discovered market mapping could not start live feeds");
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, ?backoff, "market mapping discovery will retry");
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
    });
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
    runtime.replay.event_index = 0;
    runtime.replay.status = if runtime.replay.total_events == 0 {
        ReplayStatus::Unavailable
    } else {
        ReplayStatus::Paused
    };
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
