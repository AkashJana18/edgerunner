import {
  Activity,
  AlertTriangle,
  ArrowDownLeft,
  ArrowUpRight,
  Ban,
  CircleGauge,
  Clock3,
  Copy,
  DatabaseZap,
  ExternalLink,
  FastForward,
  LogOut,
  Radio,
  Pause,
  Play,
  RotateCcw,
  RefreshCw,
  ServerCog,
  ShieldCheck,
  Wallet,
  WalletCards,
  WifiOff,
  Zap,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent } from "react";
import type {
  Decision,
  Environment,
  FeedStatus,
  MarketState,
  PositionLifecycle,
  RunMode,
  SessionState,
  Snapshot,
} from "./types";

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
  const [session, setSession] = useState<SessionState | null>(null);
  const [sessionBusy, setSessionBusy] = useState(false);
  const [replayBusy, setReplayBusy] = useState(false);
  const [tab, setTab] = useState<"decisions" | "trades">("decisions");
  const [history, setHistory] = useState<number[]>([]);

  const load = useCallback(async () => {
    setLoadState("loading");
    setError("");
    try {
      const [snapshotResponse, sessionResponse] = await Promise.all([
        fetch(`${API}/api/snapshot`),
        fetch(`${API}/api/session`),
      ]);
      if (!snapshotResponse.ok) throw new Error(`API returned ${snapshotResponse.status}`);
      if (!sessionResponse.ok) throw new Error(`Session API returned ${sessionResponse.status}`);
      const data = (await snapshotResponse.json()) as Snapshot;
      const source = (await sessionResponse.json()) as SessionState;
      setSnapshot(data);
      setSession(source);
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
    const refreshSession = async () => {
      const response = await fetch(`${API}/api/session`);
      if (response.ok) setSession((await response.json()) as SessionState);
    };
    const interval = window.setInterval(() => void refreshSession(), 1_000);
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

  const changeSession = async (patch: { environment?: Environment; run_mode?: RunMode }) => {
    setSessionBusy(true);
    setControlError("");
    try {
      const response = await fetch(`${API}/api/session`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-token": CONTROL_TOKEN,
        },
        body: JSON.stringify(patch),
      });
      const body = (await response.json().catch(() => null)) as SessionState | { error?: string } | null;
      if (!response.ok) {
        throw new Error(body && "error" in body ? body.error : "Session change was rejected.");
      }
      setSession(body as SessionState);
      setHistory([]);
    } catch (cause) {
      setControlError(cause instanceof Error ? cause.message : "Session change failed.");
    } finally {
      setSessionBusy(false);
    }
  };

  const replayCommand = async (command: Record<string, unknown>) => {
    setReplayBusy(true);
    setControlError("");
    try {
      const response = await fetch(`${API}/api/replay`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-token": CONTROL_TOKEN,
        },
        body: JSON.stringify(command),
      });
      const body = (await response.json().catch(() => null)) as SessionState | { error?: string } | null;
      if (!response.ok) {
        throw new Error(body && "error" in body ? body.error : "Replay command was rejected.");
      }
      setSession(body as SessionState);
      if (command.action === "reset" || command.action === "seek") setHistory([]);
    } catch (cause) {
      setControlError(cause instanceof Error ? cause.message : "Replay command failed.");
    } finally {
      setReplayBusy(false);
    }
  };

  if (loadState === "loading") return <LoadingView />;
  if (loadState === "error" || !snapshot || !session) {
    return <ErrorView message={error} onRetry={() => void load()} />;
  }

  const market = snapshot.markets[0];
  return (
    <div className="min-h-screen bg-background text-foreground">
      {!connected && <OfflineBanner />}
      <Header
        snapshot={snapshot}
        session={session}
        busy={controlBusy}
        sessionBusy={sessionBusy}
        onControl={setKilled}
        onSession={changeSession}
      />
      <main className="mx-auto w-full max-w-[1440px] px-4 py-5 md:px-6 lg:px-8">
        {controlError && (
          <div className="mb-4 flex items-center gap-2 border border-danger/40 bg-danger/10 px-4 py-3 text-sm text-danger" role="alert">
            <AlertTriangle size={16} aria-hidden="true" />
            {controlError}
          </div>
        )}
        <StatusStrip snapshot={snapshot} market={market} session={session} />
        {session.run_mode === "replay" && (
          <ReplayToolbar session={session} busy={replayBusy} onCommand={replayCommand} />
        )}
        <div className="mt-5 grid gap-5 xl:grid-cols-[minmax(0,1.5fr)_minmax(320px,0.75fr)]">
          <div className="min-w-0 space-y-5">
            <MarketPanel market={market} display={session.market} history={history} lifecycle={snapshot.position_lifecycle} />
            <ActivityPanel snapshot={snapshot} tab={tab} onTab={setTab} />
          </div>
          <aside className="min-w-0 space-y-5">
            <LatencyPanel snapshot={snapshot} />
            <RiskPanel snapshot={snapshot} market={market} session={session} />
            <RunPanel snapshot={snapshot} session={session} />
          </aside>
        </div>
      </main>
    </div>
  );
}

function Header({
  snapshot,
  session,
  busy,
  sessionBusy,
  onControl,
  onSession,
}: {
  snapshot: Snapshot;
  session: SessionState;
  busy: boolean;
  sessionBusy: boolean;
  onControl: (killed: boolean) => Promise<void>;
  onSession: (patch: { environment?: Environment; run_mode?: RunMode }) => Promise<void>;
}) {
  const feedStatus = snapshot.feed_status.txline;
  const liveReady = session.run_mode === "live" && session.live_available && feedStatus === "live";
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
        <div className="flex flex-wrap items-center justify-end gap-4">
          <SessionSelector
            ariaLabel="Environment"
            value={session.environment}
            options={["devnet", "mainnet"]}
            disabled={sessionBusy}
            onChange={(environment) => void onSession({ environment })}
          />
          <SessionSelector
            ariaLabel="Run mode"
            value={session.run_mode}
            options={["live", "replay"]}
            disabled={sessionBusy}
            onChange={(run_mode) => void onSession({ run_mode })}
          />
          <div className="feed-status-control" title={session.run_mode === "replay" ? "Market events are coming from the selected recording" : "Live market feed status"}>
            <Radio size={15} className={liveReady ? "text-success" : "text-muted"} aria-hidden="true" />
            <span className="text-xs text-secondary">TxLINE</span>
            <span className={`font-mono text-xs ${liveReady ? "text-success" : "text-muted"}`}>
              {session.run_mode === "replay" ? "REPLAY" : session.live_available ? feedStatus.toUpperCase() : session.mapping_status.toUpperCase()}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <WalletConnect environment={session.environment} />
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
      </div>
    </header>
  );
}

function WalletConnect({ environment }: { environment: Environment }) {
  const [address, setAddress] = useState<string | null>(null);
  const [connecting, setConnecting] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const [walletError, setWalletError] = useState("");
  const containerRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const provider = window.solana;
    if (!provider) return;

    const setConnectedAddress = (publicKey?: { toString(): string } | null) => {
      const nextAddress = publicKey === null
        ? null
        : publicKey?.toString() ?? provider.publicKey?.toString() ?? null;
      setAddress(nextAddress);
      if (!nextAddress) setMenuOpen(false);
    };
    const clearConnection = () => {
      setAddress(null);
      setMenuOpen(false);
    };

    void provider
      .connect({ onlyIfTrusted: true })
      .then(({ publicKey }) => setConnectedAddress(publicKey))
      .catch(() => undefined);
    provider.on?.("connect", setConnectedAddress);
    provider.on?.("accountChanged", setConnectedAddress);
    provider.on?.("disconnect", clearConnection);

    return () => {
      provider.removeListener?.("connect", setConnectedAddress);
      provider.removeListener?.("accountChanged", setConnectedAddress);
      provider.removeListener?.("disconnect", clearConnection);
    };
  }, []);

  useEffect(() => {
    if (!menuOpen) return;
    const dismissOnOutsideClick = (event: PointerEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) setMenuOpen(false);
    };
    const dismissOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setMenuOpen(false);
      triggerRef.current?.focus();
    };
    document.addEventListener("pointerdown", dismissOnOutsideClick);
    document.addEventListener("keydown", dismissOnEscape);
    return () => {
      document.removeEventListener("pointerdown", dismissOnOutsideClick);
      document.removeEventListener("keydown", dismissOnEscape);
    };
  }, [menuOpen]);

  useEffect(() => {
    if (!menuOpen) return;
    const frame = window.requestAnimationFrame(() => {
      menuRef.current?.querySelector<HTMLElement>("[role='menuitem']")?.focus();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [menuOpen]);

  const navigateMenu = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) return;
    const items = Array.from(event.currentTarget.querySelectorAll<HTMLElement>("[role='menuitem']"));
    if (items.length === 0) return;
    event.preventDefault();
    const currentIndex = items.indexOf(document.activeElement as HTMLElement);
    const nextIndex = event.key === "Home"
      ? 0
      : event.key === "End"
        ? items.length - 1
        : event.key === "ArrowDown"
          ? (currentIndex + 1) % items.length
          : (currentIndex - 1 + items.length) % items.length;
    items[nextIndex]?.focus();
  };

  const connect = async () => {
    const provider = window.solana;
    setWalletError("");
    setCopied(false);
    if (!provider) {
      setWalletError("No Solana wallet detected. Install Phantom, then reload.");
      window.open("https://phantom.com/download", "_blank", "noopener,noreferrer");
      return;
    }
    setConnecting(true);
    try {
      const { publicKey } = await provider.connect();
      setAddress(publicKey.toString());
    } catch (cause) {
      const rejected = typeof cause === "object" && cause !== null && "code" in cause && cause.code === 4001;
      if (!rejected) setWalletError("Wallet connection failed. Unlock your wallet and try again.");
    } finally {
      setConnecting(false);
    }
  };

  const copyAddress = async () => {
    if (!address) return;
    try {
      await navigator.clipboard.writeText(address);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1_500);
    } catch {
      setWalletError("Could not copy the wallet address.");
    }
  };

  const disconnect = async () => {
    setWalletError("");
    try {
      await window.solana?.disconnect();
      setAddress(null);
      setMenuOpen(false);
    } catch {
      setWalletError("Could not disconnect the wallet. Try again.");
    }
  };

  const explorerUrl = address
    ? `https://explorer.solana.com/address/${address}${environment === "devnet" ? "?cluster=devnet" : ""}`
    : "";

  return (
    <div ref={containerRef} className="wallet-control">
      <button
        ref={triggerRef}
        type="button"
        className={`wallet-button ${address ? "wallet-button-connected" : ""}`}
        disabled={connecting}
        aria-busy={connecting}
        aria-expanded={address ? menuOpen : undefined}
        aria-haspopup={address ? "menu" : undefined}
        onClick={() => address ? setMenuOpen((open) => !open) : void connect()}
      >
        {address ? <span className="wallet-status-dot" aria-hidden="true" /> : <Wallet size={15} aria-hidden="true" />}
        <span className={address ? "font-mono tabular-nums" : ""}>
          {connecting ? "Connecting…" : address ? truncateAddress(address) : "Connect wallet"}
        </span>
      </button>

      {address && menuOpen && (
        <div ref={menuRef} className="wallet-menu" role="menu" aria-label="Wallet actions" onKeyDown={navigateMenu}>
          <div className="wallet-menu-address" title={address}>
            <span className="wallet-status-dot" aria-hidden="true" />
            <span className="truncate font-mono">{truncateAddress(address, 6)}</span>
          </div>
          <button type="button" role="menuitem" onClick={() => void copyAddress()}>
            <Copy size={14} aria-hidden="true" />{copied ? "Copied" : "Copy address"}
          </button>
          <a href={explorerUrl} target="_blank" rel="noreferrer" role="menuitem">
            <ExternalLink size={14} aria-hidden="true" />View on explorer
          </a>
          <button type="button" role="menuitem" className="wallet-menu-disconnect" onClick={() => void disconnect()}>
            <LogOut size={14} aria-hidden="true" />Disconnect
          </button>
          {walletError && <p className="wallet-menu-error" role="alert">{walletError}</p>}
        </div>
      )}
      {walletError && !menuOpen && <p className="wallet-error" role="alert">{walletError}</p>}
    </div>
  );
}

function truncateAddress(address: string, characters = 4) {
  if (address.length <= characters * 2 + 3) return address;
  return `${address.slice(0, characters)}...${address.slice(-characters)}`;
}

function SessionSelector<T extends Environment | RunMode>({
  ariaLabel,
  value,
  options,
  disabled,
  onChange,
}: {
  ariaLabel: string;
  value: T;
  options: readonly T[];
  disabled: boolean;
  onChange: (value: T) => void;
}) {
  return (
    <div className="session-selector">
      <div className="segmented segmented-compact" role="tablist" aria-label={ariaLabel}>
        {options.map((option) => (
          <button
            key={option}
            type="button"
            role="tab"
            aria-selected={value === option}
            disabled={disabled}
            onClick={() => onChange(option)}
          >
            {option.toUpperCase()}
          </button>
        ))}
      </div>
    </div>
  );
}

function ReplayToolbar({
  session,
  busy,
  onCommand,
}: {
  session: SessionState;
  busy: boolean;
  onCommand: (command: Record<string, unknown>) => Promise<void>;
}) {
  const replay = session.replay;
  const unavailable = replay.status === "unavailable";
  const playing = replay.status === "playing";
  const progress = replay.total_events === 0 ? 0 : (replay.event_index / replay.total_events) * 100;
  return (
    <section className="panel replay-toolbar" aria-labelledby="replay-heading">
      <div className="replay-toolbar-copy">
        <div className="flex items-center gap-2">
          <FastForward size={16} className="text-secondary" aria-hidden="true" />
          <p className="eyebrow">Replay control</p>
        </div>
        <h2 id="replay-heading" className="mt-1 text-base font-semibold">Recorded session</h2>
        <p className="mt-1 truncate text-xs text-muted" title={replay.journal}>{replay.journal}</p>
      </div>
      <div className="replay-toolbar-controls">
        <button
          type="button"
          className="secondary-button"
          aria-label={playing ? "Pause replay" : "Play replay"}
          disabled={busy || unavailable}
          aria-busy={busy}
          onClick={() => void onCommand({ action: playing ? "pause" : "play" })}
        >
          {playing ? <Pause size={15} aria-hidden="true" /> : <Play size={15} aria-hidden="true" />}
          {playing ? "Pause" : "Play"}
        </button>
        <button
          type="button"
          className="secondary-button"
          aria-label="Reset replay"
          disabled={busy || unavailable}
          onClick={() => void onCommand({ action: "reset" })}
        >
          <RotateCcw size={15} aria-hidden="true" />
          Reset
        </button>
        <label className="replay-speed">
          <span>Speed</span>
          <select
            value={replay.speed}
            disabled={busy || unavailable}
            aria-label="Replay speed"
            onChange={(event) => void onCommand({ action: "set_speed", speed: Number(event.target.value) })}
          >
            {[0.5, 1, 2, 5, 10].map((speed) => <option key={speed} value={speed}>{speed}×</option>)}
          </select>
        </label>
        <span className="replay-counter font-mono" aria-live="polite">
          {replay.event_index.toLocaleString()} / {replay.total_events.toLocaleString()}
        </span>
      </div>
      <div className="replay-progress-row">
        <input
          type="range"
          min="0"
          max={replay.total_events}
          value={replay.event_index}
          disabled={busy || unavailable || replay.total_events === 0}
          aria-label="Replay position"
          onChange={(event) => void onCommand({ action: "seek", event_index: Number(event.target.value) })}
        />
        <span className={`replay-status replay-status-${replay.status}`}>{replay.status}</span>
        <span className="replay-progress-value font-mono">{progress.toFixed(0)}%</span>
      </div>
      {unavailable && (
        <p className="replay-empty" role="status">No recorded market events are available at {replay.journal}.</p>
      )}
    </section>
  );
}

function StatusStrip({ snapshot, market, session }: { snapshot: Snapshot; market?: MarketState; session: SessionState }) {
  const feedDetail = session.run_mode === "replay"
    ? "recorded journal"
    : session.live_available
      ? "TxLINE SSE"
      : session.mapping_status === "discovering"
        ? "resolving market"
        : "live source unavailable";
  const opportunity = grossOpportunity(market);
  const orderRequirement = collateralMetric(snapshot.next_order_requirement);
  const capacity = snapshot.risk_capacity;
  const pnlTone = (market?.pnl_micros ?? 0) < 0 ? "text-danger" : "";
  const metrics = [
    { label: "Fair value", value: formatProbability(market?.fair_value), detail: feedDetail, icon: DatabaseZap, valueTone: "", detailTone: "text-muted" },
    { label: "Best market", value: `${formatProbability(market?.best_bid)} / ${formatProbability(market?.best_ask)}`, detail: "bid / ask", icon: Activity, valueTone: "", detailTone: "text-muted" },
    { label: "Gross edge", value: formatProbability(opportunity.edge), detail: opportunity.detail, icon: Zap, valueTone: opportunity.tone, detailTone: opportunity.tone || "text-muted" },
    { label: "Projected collateral", value: orderRequirement.value, detail: orderRequirement.detail, icon: WalletCards, valueTone: "", detailTone: orderRequirement.tone },
    { label: "Mark-to-market", value: formatMoney(market?.pnl_micros), detail: "simulated P&L", icon: ServerCog, valueTone: pnlTone, detailTone: "text-muted" },
    {
      label: "Position",
      value: `${market?.position ?? 0}`,
      detail: capacity
        ? `${capacity.remaining_contracts} remaining · ${capacity.limiting_gate} cap`
        : "contracts",
      icon: CircleGauge,
      valueTone: "",
      detailTone: capacity?.remaining_contracts === 0 && Math.abs(market?.position ?? 0) > 0 ? "text-success" : "text-muted",
    },
  ];
  return (
    <section className="grid grid-cols-2 gap-px border border-border bg-border md:grid-cols-3 xl:grid-cols-6" aria-label="Engine status">
      {metrics.map(({ label, value, detail, icon: Icon, valueTone, detailTone }) => (
        <div key={label} className="metric-cell">
          <div className="flex items-center justify-between gap-2 text-secondary">
            <span className="text-xs font-medium">{label}</span>
            <Icon size={15} aria-hidden="true" />
          </div>
          <div className={`mt-3 truncate font-mono text-lg font-semibold tabular-nums ${valueTone}`}>{value}</div>
          <div className={`mt-1 truncate text-xs ${detailTone}`}>{detail}</div>
        </div>
      ))}
    </section>
  );
}

function MarketPanel({ market, display, history, lifecycle }: { market?: MarketState; display: SessionState["market"]; history: number[]; lifecycle: PositionLifecycle }) {
  if (!market) {
    return <EmptyPanel title="Waiting for a matched market" detail="The service is resolving live TxLINE and Pascal market metadata." />;
  }
  const readable = display ?? humanizeMarketSymbol(market.market);
  const max = Math.max(...history, 1);
  const min = Math.min(...history, max);
  return (
    <section className="panel" aria-labelledby="market-heading">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Primary market</p>
          <h2 id="market-heading" className="mt-1 text-base font-semibold">{readable.event}</h2>
          <p className="mt-1 text-sm text-secondary">{readable.contract}</p>
        </div>
        <div className="text-right">
          <p className="font-mono text-sm font-semibold">{market.phase || "PRE"} {market.score_home}-{market.score_away}</p>
          <p className="mt-1 text-xs text-muted">{friendlyPeriod(readable.period)}</p>
        </div>
      </div>
      <PositionLifecycleStrip lifecycle={lifecycle} />
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
          <div className="flex items-center justify-between gap-2">
            <p className="eyebrow">Best available prices</p>
            <p className="text-[11px] text-muted">Price / size</p>
          </div>
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

function PositionLifecycleStrip({ lifecycle }: { lifecycle: PositionLifecycle }) {
  const realizedTone = lifecycle.realized_pnl_micros < 0 ? "text-danger" : lifecycle.realized_pnl_micros > 0 ? "text-success" : "";
  const values = [
    { label: "Position status", value: lifecycle.status, tone: lifecycle.status === "OPEN" ? "text-success" : "text-secondary" },
    { label: "Entry price", value: formatProbability(lifecycle.entry_price), tone: "" },
    { label: "Exit price", value: formatProbability(lifecycle.exit_price), tone: "" },
    { label: "Holding time", value: formatHoldingTime(lifecycle.holding_time_ns), tone: "" },
    { label: "Realized P&L", value: formatMoney(lifecycle.realized_pnl_micros), tone: realizedTone },
  ];
  return (
    <dl className="grid grid-cols-2 gap-px border-y border-border bg-border sm:grid-cols-3 xl:grid-cols-5" aria-label="Position lifecycle">
      {values.map(({ label, value, tone }) => (
        <div key={label} className="bg-surface px-4 py-3 md:px-5">
          <dt className="text-[11px] text-muted">{label}</dt>
          <dd className={`mt-1 truncate font-mono text-sm font-semibold tabular-nums ${tone}`}>{value}</dd>
        </div>
      ))}
    </dl>
  );
}

function humanizeMarketSymbol(symbol: string): NonNullable<SessionState["market"]> {
  const countries: Record<string, string> = {
    ARG: "Argentina",
    ESP: "Spain",
    FRA: "France",
  };
  const matchup = symbol.match(/_(?:[A-Z]+_)?([A-Z]{3})([A-Z]{3})_/);
  const home = matchup ? countries[matchup[1]] ?? matchup[1] : "Primary";
  const away = matchup ? countries[matchup[2]] ?? matchup[2] : "market";
  const teamTotal = symbol.match(/_([A-Z]{3})TT\./);
  const team = teamTotal ? countries[teamTotal[1]] ?? teamTotal[1] : null;
  const contract = symbol.endsWith("ML.DRAW")
    ? "Draw"
    : team
      ? `${team} team total`
      : "Selected contract";
  return { event: `${home} vs ${away}`, contract, period: "Regulation time", starts_at_ms: null };
}

function friendlyPeriod(period: string) {
  if (/reg(?:ulation)?\s*time/i.test(period)) return "Regulation time";
  return period.replace(/\s*-\s*/g, " · ");
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

function ActivityPanel({ snapshot, tab, onTab }: { snapshot: Snapshot; tab: "decisions" | "trades"; onTab: (tab: "decisions" | "trades") => void }) {
  return (
    <section className="panel" aria-labelledby="activity-heading">
      <div className="panel-heading flex-wrap">
        <div>
          <p className="eyebrow">Execution journal</p>
          <h2 id="activity-heading" className="mt-1 text-base font-semibold">Strategy activity</h2>
        </div>
        <div className="segmented" role="tablist" aria-label="Activity view">
          <button type="button" role="tab" aria-selected={tab === "decisions"} onClick={() => onTab("decisions")}>Decisions</button>
          <button type="button" role="tab" aria-selected={tab === "trades"} onClick={() => onTab("trades")}>Trades</button>
        </div>
      </div>
      {tab === "decisions" ? <DecisionTable decisions={snapshot.decisions} /> : <TradeTable snapshot={snapshot} />}
    </section>
  );
}

function DecisionTable({ decisions }: { decisions: Decision[] }) {
  if (decisions.length === 0) return <EmptyPanel title="No evaluations yet" detail="The engine is waiting for synchronized market data." inline />;
  return (
    <div className="table-scroll">
      <table>
        <thead><tr><th>Time</th><th>Action</th><th>Side</th><th>Price</th><th>Qty</th><th>Net edge</th><th>Compute</th><th>Reason</th></tr></thead>
        <tbody>
          {decisions.slice(0, 10).map((decision) => (
            <tr key={decision.id}>
              <td className="font-mono">{new Date(decision.at).toLocaleTimeString([], { hour12: false })}</td>
              <td><StatusPill status={decision.action} /></td>
              <td className={decision.intent?.side === "bid" ? "text-success" : "text-danger"}>{decision.intent?.side?.toUpperCase() ?? "-"}</td>
              <td className="font-mono">{formatProbability(decision.intent?.limit_price)}</td>
              <td className="font-mono">{decision.intent?.quantity ?? "-"}</td>
              <td className="font-mono">{decision.intent ? `${(decision.intent.expected_edge_micros / 10_000).toFixed(2)}%` : "-"}</td>
              <td className="font-mono">{formatLatency(decision.compute_latency_ns)}</td>
              <td className="max-w-64 truncate text-secondary" title={decision.reason}>{decision.reason}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TradeTable({ snapshot }: { snapshot: Snapshot }) {
  if (snapshot.trades.length === 0) return <EmptyPanel title="No trades yet" detail="BUY and SELL events appear when entry or exit orders fill." inline />;
  return (
    <div className="table-scroll">
      <table>
        <thead><tr><th>Time</th><th>Event</th><th>Type</th><th>Price</th><th>Edge</th><th>Qty</th><th>Realized P&amp;L</th></tr></thead>
        <tbody>{snapshot.trades.slice(0, 10).map((trade) => (
          <tr key={`${trade.order_id}-${trade.kind}`}>
            <td className="font-mono">{new Date(trade.timestamp).toLocaleTimeString([], { hour12: false })}</td>
            <td className={trade.action === "BUY" ? "text-success" : "text-danger"}>{trade.action}</td>
            <td className="font-mono text-secondary">{trade.kind.toUpperCase()}</td>
            <td className="font-mono">{formatProbability(trade.price)}</td>
            <td className="font-mono">{(trade.edge_micros / 10_000).toFixed(2)}%</td>
            <td className="font-mono">{trade.quantity}</td>
            <td className={`font-mono ${trade.realized_pnl_micros < 0 ? "text-danger" : trade.realized_pnl_micros > 0 ? "text-success" : "text-muted"}`}>{formatMoney(trade.realized_pnl_micros)}</td>
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
  const ceiling = 100;
  return (
    <section className="panel" aria-labelledby="latency-heading">
      <div className="panel-heading">
        <div><p className="eyebrow">Hot path</p><h2 id="latency-heading" className="mt-1 text-base font-semibold">Decision compute latency</h2></div>
        <Clock3 size={18} className="text-secondary" aria-hidden="true" />
      </div>
      <div className="latency-chart p-4 md:p-5">
        <div className="latency-grid" aria-label="Latency values on a fixed 0 to 100 microsecond scale">
          {[0, 25, 50, 75, 100].map((tick) => <span key={tick} style={{ left: `${tick}%` }} aria-hidden="true" />)}
          {values.map(([label, value]) => {
            const width = Math.min(100, Math.max(2, (value / ceiling) * 100));
            return (
              <div key={label} className="latency-row">
                <span className="latency-label">{label}</span>
                <div className="latency-track"><span style={{ width: `${width}%` }} /></div>
                <strong className="font-mono text-sm">{value} μs{value > ceiling ? " +" : ""}</strong>
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
}

function RiskPanel({ snapshot, market, session }: { snapshot: Snapshot; market?: MarketState; session: SessionState }) {
  const liveFeeds = Object.values(snapshot.feed_status).every((status) => status === "live");
  const replayHasData = session.run_mode === "replay" && snapshot.processed_events > 0;
  const feedOkay = session.run_mode === "replay" ? replayHasData : liveFeeds;
  const position = Math.abs(market?.position ?? 0);
  const effectiveLimit = snapshot.risk_capacity?.effective_position_limit ?? 250;
  const atCapacity = position > 0 && snapshot.risk_capacity?.remaining_contracts === 0;
  const capacityGate = snapshot.risk_capacity?.limiting_gate ?? "position";
  const states = [
    { label: "Kill switch", ok: !snapshot.killed, text: snapshot.killed ? "ACTIVE" : "armed" },
    { label: "Feed freshness", ok: feedOkay, text: session.run_mode === "replay" ? (replayHasData ? "recorded" : "waiting") : liveFeeds ? "within limit" : "unavailable" },
    { label: "Market circuit", ok: !market?.danger && !market?.suspended, text: market?.danger || market?.suspended ? "blocked" : "clear" },
    {
      label: "Entry capacity",
      ok: position <= effectiveLimit,
      text: `${position} / ${effectiveLimit} · ${atCapacity ? "full" : capacityGate}`,
    },
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

function RunPanel({ snapshot, session }: { snapshot: Snapshot; session: SessionState }) {
  const replaySource = session.run_mode === "replay";
  return (
    <section className="panel" aria-labelledby="run-heading">
      <div className="panel-heading"><div><p className="eyebrow">Runtime</p><h2 id="run-heading" className="mt-1 text-base font-semibold">Run identity</h2></div><ServerCog size={18} className="text-secondary" aria-hidden="true" /></div>
      <dl className="grid grid-cols-[92px_1fr] gap-x-3 gap-y-3 p-4 text-xs md:p-5">
        <dt className="text-muted">Run ID</dt><dd className="truncate font-mono" title={snapshot.run_id}>{snapshot.run_id}</dd>
        <dt className="text-muted">Environment</dt><dd className="font-mono uppercase">{session.environment}</dd>
        <dt className="text-muted">Run mode</dt><dd className={`font-mono uppercase ${session.run_mode === "live" ? "text-success" : "text-secondary"}`}>{session.run_mode}</dd>
        <dt className="text-muted">Events</dt><dd className="font-mono">{snapshot.processed_events.toLocaleString()}</dd>
        <dt className="text-muted">Rejected</dt><dd className="font-mono">{snapshot.rejected_orders.toLocaleString()}</dd>
        <dt className="text-muted">TxLINE</dt><dd>{replaySource ? <span className="feed-badge feed-replay">replay</span> : <FeedBadge status={snapshot.feed_status.txline} />}</dd>
        <dt className="text-muted">Pascal</dt><dd>{replaySource ? <span className="feed-badge feed-replay">replay</span> : <FeedBadge status={snapshot.feed_status.pascal} />}</dd>
      </dl>
    </section>
  );
}

function FeedBadge({ status = "disconnected" }: { status?: FeedStatus }) {
  return <span className={`feed-badge feed-${status}`}>{status}</span>;
}

function StatusPill({ status }: { status: string }) {
  const Icon = status === "submitted" ? ArrowUpRight : status === "skipped" ? CircleGauge : Ban;
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

function grossOpportunity(market?: MarketState) {
  if (market?.fair_value == null || market.best_bid == null || market.best_ask == null) {
    return { edge: null, detail: "Waiting for both feeds", tone: "" };
  }
  const buyEdge = market.fair_value - market.best_ask;
  const sellEdge = market.best_bid - market.fair_value;
  const edge = Math.max(buyEdge, sellEdge);
  if (edge <= 0) return { edge: 0, detail: "No actionable edge", tone: "" };
  return buyEdge >= sellEdge
    ? { edge: buyEdge, detail: "BUY at best ask", tone: "text-success" }
    : { edge: sellEdge, detail: "SELL at best bid", tone: "text-danger" };
}

function collateralMetric(requirement: Snapshot["next_order_requirement"]) {
  if (!requirement) return { value: "—", detail: "Waiting for strategy", tone: "text-muted" };
  const side = requirement.side === "bid" ? "BUY" : "SELL";
  const status = requirement.decision_status === "rejected"
    ? "blocked"
    : requirement.decision_status === "skipped"
      ? "not actionable"
      : requirement.decision_status;
  const tone = requirement.decision_status === "rejected"
    ? "text-danger"
    : requirement.decision_status === "submitted"
      ? "text-success"
      : "text-muted";
  return {
    value: formatMoney(requirement.required_funds_micros),
    detail: `${side} ${requirement.quantity} · ${status}`,
    tone,
  };
}

function formatLatency(ns: number) {
  return ns < 1_000 ? `${ns} ns` : `${(ns / 1_000).toFixed(1)} us`;
}

function formatHoldingTime(ns: number) {
  if (ns <= 0) return "—";
  const milliseconds = ns / 1_000_000;
  if (milliseconds < 1_000) return `${milliseconds.toFixed(milliseconds < 10 ? 1 : 0)} ms`;
  const seconds = milliseconds / 1_000;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)} s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${Math.floor(seconds % 60)}s`;
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
