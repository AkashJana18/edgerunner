import {
  Activity,
  AlertTriangle,
  ArrowDownLeft,
  ArrowUpRight,
  Ban,
  CircleGauge,
  Clock3,
  DatabaseZap,
  Pause,
  Play,
  RefreshCw,
  ServerCog,
  ShieldCheck,
  ToggleLeft,
  ToggleRight,
  WifiOff,
  Zap,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { Decision, FeedMode, FeedModeState, FeedStatus, MarketState, Snapshot } from "./types";

type LoadState = "loading" | "ready" | "error";

const API = import.meta.env.VITE_API_BASE_URL ?? "";
const CONTROL_TOKEN = import.meta.env.VITE_EDGERUNNER_CONTROL_TOKEN ?? "local-demo";

function App() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [loadState, setLoadState] = useState<LoadState>("loading");
  const [error, setError] = useState("");
  const [connected, setConnected] = useState(false);
  const [controlBusy, setControlBusy] = useState(false);
  const [controlError, setControlError] = useState("");
  const [feedMode, setFeedMode] = useState<FeedModeState | null>(null);
  const [feedBusy, setFeedBusy] = useState(false);
  const [tab, setTab] = useState<"decisions" | "fills">("decisions");
  const [history, setHistory] = useState<number[]>([]);

  const load = useCallback(async () => {
    setLoadState("loading");
    setError("");
    try {
      const [snapshotResponse, feedModeResponse] = await Promise.all([
        fetch(`${API}/api/snapshot`),
        fetch(`${API}/api/feed-mode`),
      ]);
      if (!snapshotResponse.ok) throw new Error(`API returned ${snapshotResponse.status}`);
      if (!feedModeResponse.ok) throw new Error(`Feed source returned ${feedModeResponse.status}`);
      const data = (await snapshotResponse.json()) as Snapshot;
      const source = (await feedModeResponse.json()) as FeedModeState;
      setSnapshot(data);
      setFeedMode(source);
      setLoadState("ready");
    } catch (cause) {
      setLoadState("error");
      setError(cause instanceof Error ? cause.message : "The engine did not respond.");
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (loadState !== "ready") return;
    const source = new EventSource(`${API}/api/events`);
    source.addEventListener("snapshot", (event) => {
      const next = JSON.parse((event as MessageEvent).data) as Snapshot;
      setSnapshot(next);
      const fair = next.markets[0]?.fair_value;
      if (fair != null) setHistory((current) => [...current.slice(-39), fair]);
      setConnected(true);
    });
    source.onerror = () => setConnected(false);
    source.onopen = () => setConnected(true);
    return () => source.close();
  }, [loadState]);

  useEffect(() => {
    if (loadState !== "ready") return;
    const refreshFeedMode = async () => {
      const response = await fetch(`${API}/api/feed-mode`);
      if (response.ok) setFeedMode((await response.json()) as FeedModeState);
    };
    const interval = window.setInterval(() => void refreshFeedMode(), 5_000);
    return () => window.clearInterval(interval);
  }, [loadState]);

  const setKilled = async (killed: boolean) => {
    setControlBusy(true);
    setControlError("");
    try {
      const response = await fetch(`${API}/api/${killed ? "kill" : "resume"}`, {
        method: "POST",
        headers: { "x-api-token": CONTROL_TOKEN },
      });
      if (!response.ok) throw new Error("Control request was rejected.");
    } catch (cause) {
      setControlError(cause instanceof Error ? cause.message : "Control request failed.");
    } finally {
      setControlBusy(false);
    }
  };

  const setFeedSource = async (mode: FeedMode) => {
    setFeedBusy(true);
    setControlError("");
    try {
      const response = await fetch(`${API}/api/feed-mode`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-token": CONTROL_TOKEN,
        },
        body: JSON.stringify({ mode }),
      });
      const body = (await response.json().catch(() => null)) as FeedModeState | { error?: string } | null;
      if (!response.ok) {
        throw new Error(body && "error" in body ? body.error : "Feed source change was rejected.");
      }
      setFeedMode(body as FeedModeState);
      setHistory([]);
    } catch (cause) {
      setControlError(cause instanceof Error ? cause.message : "Feed source change failed.");
    } finally {
      setFeedBusy(false);
    }
  };

  if (loadState === "loading") return <LoadingView />;
  if (loadState === "error" || !snapshot) {
    return <ErrorView message={error} onRetry={() => void load()} />;
  }

  const market = snapshot.markets[0];
  return (
    <div className="min-h-screen bg-background text-foreground">
      {!connected && <OfflineBanner />}
      <Header
        snapshot={snapshot}
        busy={controlBusy}
        feedMode={feedMode}
        feedBusy={feedBusy}
        onControl={setKilled}
        onFeedMode={setFeedSource}
      />
      <main className="mx-auto w-full max-w-[1440px] px-4 py-5 md:px-6 lg:px-8">
        {controlError && (
          <div className="mb-4 flex items-center gap-2 border border-danger/40 bg-danger/10 px-4 py-3 text-sm text-danger" role="alert">
            <AlertTriangle size={16} aria-hidden="true" />
            {controlError}
          </div>
        )}
        <StatusStrip snapshot={snapshot} market={market} feedMode={feedMode} />
        <div className="mt-5 grid gap-5 xl:grid-cols-[minmax(0,1.5fr)_minmax(320px,0.75fr)]">
          <div className="min-w-0 space-y-5">
            <MarketPanel market={market} history={history} />
            <ActivityPanel snapshot={snapshot} tab={tab} onTab={setTab} />
          </div>
          <aside className="min-w-0 space-y-5">
            <LatencyPanel snapshot={snapshot} />
            <RiskPanel snapshot={snapshot} market={market} feedMode={feedMode} />
            <RunPanel snapshot={snapshot} feedMode={feedMode} />
          </aside>
        </div>
      </main>
    </div>
  );
}

function Header({
  snapshot,
  busy,
  feedMode,
  feedBusy,
  onControl,
  onFeedMode,
}: {
  snapshot: Snapshot;
  busy: boolean;
  feedMode: FeedModeState | null;
  feedBusy: boolean;
  onControl: (killed: boolean) => Promise<void>;
  onFeedMode: (mode: FeedMode) => Promise<void>;
}) {
  const live = feedMode?.mode === "live";
  const feedSwitchDisabled = feedBusy || (!live && !feedMode?.live_available);
  const discoveryInProgress = feedMode?.mapping_status === "discovering";
  return (
    <header className="border-b border-border bg-surface">
      <div className="mx-auto flex min-h-16 w-full max-w-[1440px] flex-wrap items-center justify-between gap-3 px-4 py-3 md:px-6 lg:px-8">
        <div className="flex min-w-0 items-center gap-3">
          <div className="grid size-10 shrink-0 place-items-center bg-accent text-accent-foreground" aria-hidden="true">
            <Zap size={20} fill="currentColor" />
          </div>
          <div className="min-w-0">
            <div className="flex items-baseline gap-2">
              <h1 className="truncate text-base font-semibold">EdgeRunner</h1>
              <span className="font-mono text-xs text-muted">operator</span>
            </div>
            <p className="truncate text-xs text-secondary">Deterministic execution core</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-2 border border-border bg-background px-2 py-1.5">
            <span className="text-xs text-secondary">TxLINE</span>
            <button
              type="button"
              className={`feed-toggle ${live ? "feed-toggle-live" : ""}`}
              role="switch"
              aria-checked={live}
              aria-label={live ? "Stop live feeds" : "Start live feeds"}
              title={feedMode?.live_available ? (live ? "Stop live feeds" : "Start live feeds") : discoveryInProgress ? "Resolving a matching TxLINE fixture and Pascal market" : "Set TXLINE_API_TOKEN on the server to enable live data"}
              disabled={feedSwitchDisabled}
              aria-busy={feedBusy}
              onClick={() => void onFeedMode(live ? "inactive" : "live")}
            >
              {live ? <ToggleRight size={22} aria-hidden="true" /> : <ToggleLeft size={22} aria-hidden="true" />}
            </button>
            <span className={`font-mono text-xs ${live ? "text-success" : "text-muted"}`}>
              {feedBusy ? "SWITCHING" : live ? "LIVE" : feedMode?.live_available ? "OFF" : discoveryInProgress ? "DISCOVERING" : "SETUP"}
            </span>
          </div>
          <span className="hidden border border-border bg-background px-3 py-2 font-mono text-xs text-secondary sm:inline-flex">
            {snapshot.mode.toUpperCase()}
          </span>
          <button
            type="button"
            className={`control-button ${snapshot.killed ? "control-button-resume" : "control-button-kill"}`}
            disabled={busy}
            aria-busy={busy}
            onClick={() => void onControl(!snapshot.killed)}
          >
            {snapshot.killed ? <Play size={16} aria-hidden="true" /> : <Pause size={16} aria-hidden="true" />}
            {busy ? "Applying" : snapshot.killed ? "Resume" : "Kill engine"}
          </button>
        </div>
      </div>
    </header>
  );
}

function StatusStrip({ snapshot, market, feedMode }: { snapshot: Snapshot; market?: MarketState; feedMode: FeedModeState | null }) {
  const feedDetail = feedMode?.mode === "live"
    ? "TxLINE SSE"
    : feedMode?.mapping_status === "discovering"
      ? "resolving market"
      : "feeds inactive";
  const metrics = [
    { label: "Fair value", value: formatProbability(market?.fair_value), detail: feedDetail, icon: DatabaseZap },
    { label: "Best market", value: `${formatProbability(market?.best_bid)} / ${formatProbability(market?.best_ask)}`, detail: "bid / ask", icon: Activity },
    { label: "Position", value: `${market?.position ?? 0}`, detail: "contracts", icon: CircleGauge },
    { label: "Mark-to-market", value: formatMoney(market?.pnl_micros), detail: `${compact(snapshot.processed_events)} events`, icon: ServerCog },
  ];
  return (
    <section className="grid grid-cols-2 border border-border bg-surface lg:grid-cols-4" aria-label="Engine status">
      {metrics.map(({ label, value, detail, icon: Icon }) => (
        <div key={label} className="metric-cell">
          <div className="flex items-center justify-between gap-2 text-secondary">
            <span className="text-xs font-medium">{label}</span>
            <Icon size={15} aria-hidden="true" />
          </div>
          <div className="mt-3 truncate font-mono text-lg font-semibold tabular-nums">{value}</div>
          <div className="mt-1 text-xs text-muted">{detail}</div>
        </div>
      ))}
    </section>
  );
}

function MarketPanel({ market, history }: { market?: MarketState; history: number[] }) {
  if (!market) {
    return <EmptyPanel title="Waiting for a matched market" detail="The service is resolving live TxLINE and Pascal market metadata." />;
  }
  const max = Math.max(...history, 1);
  const min = Math.min(...history, max);
  return (
    <section className="panel" aria-labelledby="market-heading">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Primary market</p>
          <h2 id="market-heading" className="mt-1 text-base font-semibold">{market.market}</h2>
        </div>
        <div className="text-right">
          <p className="font-mono text-sm font-semibold">{market.phase || "PRE"} {market.score_home}-{market.score_away}</p>
          <p className="mt-1 text-xs text-muted">{market.market}</p>
        </div>
      </div>
      <div className="grid gap-4 p-4 md:grid-cols-[minmax(0,1fr)_220px] md:p-5">
        <div className="min-w-0">
          <div className="flex items-baseline justify-between gap-3">
            <div>
              <p className="text-xs text-secondary">TxLINE probability</p>
              <p className="mt-1 font-mono text-3xl font-semibold tabular-nums">{formatProbability(market.fair_value)}</p>
            </div>
            <div className="text-right text-xs">
              <p className="text-secondary">Spread</p>
              <p className="mt-1 font-mono text-sm text-foreground">{formatSpread(market)}</p>
            </div>
          </div>
          <div className="mt-6 flex h-32 items-end gap-1 border-b border-l border-border px-2 pt-3" aria-label="Recent fair value samples">
            {history.length === 0 ? (
              <p className="m-auto text-xs text-muted">Waiting for price samples</p>
            ) : (
              history.map((value, index) => {
                const height = 28 + ((value - min) / Math.max(max - min, 1)) * 68;
                return <span key={`${index}-${value}`} className="price-bar" style={{ height: `${height}%` }} />;
              })
            )}
          </div>
          <div className="mt-2 flex justify-between font-mono text-[12px] text-muted"><span>-14s</span><span>now</span></div>
        </div>
        <div className="border-t border-border pt-4 md:border-l md:border-t-0 md:pl-4 md:pt-0">
          <p className="eyebrow">Top of book</p>
          <BookRow label="Ask" price={market.best_ask} size={market.ask_size} side="ask" />
          <BookRow label="Bid" price={market.best_bid} size={market.bid_size} side="bid" />
          <div className="mt-5 grid grid-cols-2 gap-2 text-xs">
            <div className="data-block"><span>Source seq</span><strong>{market.source_seq}</strong></div>
            <div className="data-block"><span>Venue seq</span><strong>{market.venue_seq}</strong></div>
          </div>
        </div>
      </div>
    </section>
  );
}

function BookRow({ label, price, size, side }: { label: string; price: number | null; size: number; side: "bid" | "ask" }) {
  return (
    <div className="mt-3 grid grid-cols-[48px_1fr_52px] items-center gap-2 border-b border-border pb-3 font-mono text-sm">
      <span className={side === "bid" ? "text-success" : "text-danger"}>{label}</span>
      <strong className="text-right">{formatProbability(price)}</strong>
      <span className="text-right text-muted">{size}</span>
    </div>
  );
}

function ActivityPanel({ snapshot, tab, onTab }: { snapshot: Snapshot; tab: "decisions" | "fills"; onTab: (tab: "decisions" | "fills") => void }) {
  return (
    <section className="panel" aria-labelledby="activity-heading">
      <div className="panel-heading flex-wrap">
        <div>
          <p className="eyebrow">Execution journal</p>
          <h2 id="activity-heading" className="mt-1 text-base font-semibold">Strategy activity</h2>
        </div>
        <div className="segmented" role="tablist" aria-label="Activity view">
          <button type="button" role="tab" aria-selected={tab === "decisions"} onClick={() => onTab("decisions")}>Decisions</button>
          <button type="button" role="tab" aria-selected={tab === "fills"} onClick={() => onTab("fills")}>Fills</button>
        </div>
      </div>
      {tab === "decisions" ? <DecisionTable decisions={snapshot.decisions} /> : <FillTable snapshot={snapshot} />}
    </section>
  );
}

function DecisionTable({ decisions }: { decisions: Decision[] }) {
  if (decisions.length === 0) return <EmptyPanel title="No decisions yet" detail="The engine is waiting for an executable edge." inline />;
  return (
    <div className="table-scroll">
      <table>
        <thead><tr><th>Time</th><th>Action</th><th>Side</th><th>Price</th><th>Qty</th><th>Edge</th><th>Core</th></tr></thead>
        <tbody>
          {decisions.slice(0, 10).map((decision) => (
            <tr key={decision.id}>
              <td className="font-mono">{new Date(decision.at).toLocaleTimeString([], { hour12: false })}</td>
              <td><StatusPill status={decision.action} /></td>
              <td className={decision.intent?.side === "bid" ? "text-success" : "text-danger"}>{decision.intent?.side?.toUpperCase() ?? "-"}</td>
              <td className="font-mono">{formatProbability(decision.intent?.limit_price)}</td>
              <td className="font-mono">{decision.intent?.quantity ?? "-"}</td>
              <td className="font-mono">{decision.intent ? `${(decision.intent.expected_edge_micros / 10_000).toFixed(2)}%` : "-"}</td>
              <td className="font-mono">{formatLatency(decision.decision_latency_ns)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FillTable({ snapshot }: { snapshot: Snapshot }) {
  if (snapshot.fills.length === 0) return <EmptyPanel title="No fills yet" detail="Paper fills appear after an intent crosses visible depth." inline />;
  return (
    <div className="table-scroll">
      <table>
        <thead><tr><th>Order</th><th>Side</th><th>Price</th><th>Qty</th><th>Fee</th></tr></thead>
        <tbody>{snapshot.fills.slice(0, 10).map((fill) => (
          <tr key={fill.order_id}>
            <td className="font-mono text-muted">{fill.order_id.slice(0, 8)}</td>
            <td className={fill.side === "bid" ? "text-success" : "text-danger"}>{fill.side.toUpperCase()}</td>
            <td className="font-mono">{formatProbability(fill.price)}</td>
            <td className="font-mono">{fill.quantity}</td>
            <td className="font-mono">${(fill.fee_micros / 1_000_000).toFixed(4)}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  );
}

function LatencyPanel({ snapshot }: { snapshot: Snapshot }) {
  const values = [
    ["p50", snapshot.latency.p50_us],
    ["p95", snapshot.latency.p95_us],
    ["p99", snapshot.latency.p99_us],
    ["max", snapshot.latency.max_us],
  ] as const;
  const max = Math.max(snapshot.latency.max_us, 1);
  return (
    <section className="panel" aria-labelledby="latency-heading">
      <div className="panel-heading">
        <div><p className="eyebrow">Hot path</p><h2 id="latency-heading" className="mt-1 text-base font-semibold">Decision latency</h2></div>
        <Clock3 size={18} className="text-secondary" aria-hidden="true" />
      </div>
      <div className="space-y-4 p-4 md:p-5">
        {values.map(([label, value]) => (
          <div key={label}>
            <div className="flex items-baseline justify-between gap-3 text-xs"><span className="text-secondary">{label}</span><strong className="font-mono text-sm">{value} us</strong></div>
            <div className="latency-track"><span style={{ width: `${Math.max(4, (value / max) * 100)}%` }} /></div>
          </div>
        ))}
        <p className="border-t border-border pt-3 text-xs text-muted">{snapshot.latency.samples.toLocaleString()} measured decisions</p>
      </div>
    </section>
  );
}

function RiskPanel({ snapshot, market, feedMode }: { snapshot: Snapshot; market?: MarketState; feedMode: FeedModeState | null }) {
  const liveFeeds = Object.values(snapshot.feed_status).every((status) => status === "live");
  const inactive = feedMode?.mode === "inactive";
  const states = [
    { label: "Kill switch", ok: !snapshot.killed, text: snapshot.killed ? "ACTIVE" : "armed" },
    { label: "Feed freshness", ok: liveFeeds, text: inactive ? "inactive" : liveFeeds ? "within limit" : "unavailable" },
    { label: "Market circuit", ok: !market?.danger && !market?.suspended, text: market?.danger || market?.suspended ? "blocked" : "clear" },
    { label: "Position limit", ok: Math.abs(market?.position ?? 0) < 250, text: `${Math.abs(market?.position ?? 0)} / 250` },
  ];
  return (
    <section className="panel" aria-labelledby="risk-heading">
      <div className="panel-heading">
        <div><p className="eyebrow">Circuit-0</p><h2 id="risk-heading" className="mt-1 text-base font-semibold">Risk gates</h2></div>
        <ShieldCheck size={18} className="text-secondary" aria-hidden="true" />
      </div>
      <div className="divide-y divide-border px-4 md:px-5">
        {states.map((state) => (
          <div key={state.label} className="flex min-h-12 items-center justify-between gap-3 py-2 text-sm">
            <span className="text-secondary">{state.label}</span>
            <span className={`flex items-center gap-2 font-mono text-xs ${state.ok ? "text-success" : "text-danger"}`}>
              <span className={`size-1.5 ${state.ok ? "bg-success" : "bg-danger"}`} aria-hidden="true" />{state.text}
            </span>
          </div>
        ))}
      </div>
    </section>
  );
}

function RunPanel({ snapshot, feedMode }: { snapshot: Snapshot; feedMode: FeedModeState | null }) {
  return (
    <section className="panel" aria-labelledby="run-heading">
      <div className="panel-heading"><div><p className="eyebrow">Runtime</p><h2 id="run-heading" className="mt-1 text-base font-semibold">Run identity</h2></div><ServerCog size={18} className="text-secondary" aria-hidden="true" /></div>
      <dl className="grid grid-cols-[92px_1fr] gap-x-3 gap-y-3 p-4 text-xs md:p-5">
        <dt className="text-muted">Run ID</dt><dd className="truncate font-mono" title={snapshot.run_id}>{snapshot.run_id}</dd>
        <dt className="text-muted">Mode</dt><dd className="font-mono uppercase">{snapshot.mode}</dd>
        <dt className="text-muted">Source</dt><dd className={`font-mono uppercase ${feedMode?.mode === "live" ? "text-success" : "text-secondary"}`}>{feedMode?.mode ?? "unknown"}</dd>
        <dt className="text-muted">Events</dt><dd className="font-mono">{snapshot.processed_events.toLocaleString()}</dd>
        <dt className="text-muted">Rejected</dt><dd className="font-mono">{snapshot.rejected_orders.toLocaleString()}</dd>
        <dt className="text-muted">TxLINE</dt><dd><FeedBadge status={snapshot.feed_status.txline} /></dd>
        <dt className="text-muted">Pascal</dt><dd><FeedBadge status={snapshot.feed_status.pascal} /></dd>
      </dl>
    </section>
  );
}

function FeedBadge({ status = "disconnected" }: { status?: FeedStatus }) {
  return <span className={`feed-badge feed-${status}`}>{status}</span>;
}

function StatusPill({ status }: { status: string }) {
  const Icon = status === "submitted" ? ArrowUpRight : Ban;
  return <span className={`status-pill status-${status}`}><Icon size={13} aria-hidden="true" />{status}</span>;
}

function LoadingView() {
  return (
    <div className="min-h-screen bg-background p-4 text-foreground md:p-6" aria-busy="true" aria-label="Loading engine state">
      <div className="mx-auto max-w-[1440px] space-y-5">
        <div className="h-16 animate-pulse bg-surface" />
        <div className="grid grid-cols-2 gap-px bg-border lg:grid-cols-4">{Array.from({ length: 4 }).map((_, i) => <div key={i} className="h-28 animate-pulse bg-surface" />)}</div>
        <div className="grid gap-5 xl:grid-cols-[1.5fr_0.75fr]"><div className="h-96 animate-pulse bg-surface" /><div className="h-96 animate-pulse bg-surface" /></div>
      </div>
    </div>
  );
}

function ErrorView({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <main className="grid min-h-screen place-items-center bg-background p-6 text-foreground">
      <section className="w-full max-w-md border border-danger/40 bg-surface p-6">
        <AlertTriangle className="text-danger" aria-hidden="true" />
        <h1 className="mt-5 text-lg font-semibold">Engine unavailable</h1>
        <p className="mt-2 text-sm leading-6 text-secondary">{message || "The operator API could not be reached."}</p>
        <button type="button" className="secondary-button mt-5" onClick={onRetry}><RefreshCw size={16} aria-hidden="true" />Retry</button>
      </section>
    </main>
  );
}

function OfflineBanner() {
  return <div className="flex min-h-10 items-center justify-center gap-2 bg-warning px-4 py-2 text-center text-xs font-medium text-warning-foreground" role="status"><WifiOff size={15} aria-hidden="true" />Dashboard stream reconnecting. Last confirmed state remains visible.</div>;
}

function EmptyPanel({ title, detail, inline = false }: { title: string; detail: string; inline?: boolean }) {
  return <div className={`${inline ? "min-h-48" : "min-h-64 border border-border bg-surface"} grid place-items-center p-6 text-center`}><div><ArrowDownLeft className="mx-auto text-muted" aria-hidden="true" /><p className="mt-3 text-sm font-medium">{title}</p><p className="mt-1 text-xs text-muted">{detail}</p></div></div>;
}

function formatProbability(value?: number | null) {
  return value == null ? "-" : `${(value / 10_000).toFixed(2)}%`;
}

function formatSpread(market?: MarketState) {
  if (market?.best_ask == null || market.best_bid == null) return "-";
  return `${((market.best_ask - market.best_bid) / 10_000).toFixed(2)}%`;
}

function formatLatency(ns: number) {
  return ns < 1_000 ? `${ns} ns` : `${(ns / 1_000).toFixed(1)} us`;
}

function formatMoney(value?: number | null) {
  if (value == null) return "$0.00";
  const amount = value / 1_000_000;
  return `${amount < 0 ? "-" : ""}$${Math.abs(amount).toFixed(2)}`;
}

function compact(value: number) {
  return Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

export default App;
