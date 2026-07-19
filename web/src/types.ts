export type FeedStatus = "connecting" | "live" | "stale" | "disconnected";
export type FeedMode = "inactive" | "live";
export type MappingStatus = "unavailable" | "discovering" | "ready";
export type Side = "bid" | "ask";
export type OrderIntentKind = "entry" | "exit";
export type TradeAction = "BUY" | "SELL";
export type PositionStatus = "OPEN" | "CLOSED";
export type Environment = "devnet" | "mainnet";
export type RunMode = "live" | "replay";
export type ReplayStatus = "paused" | "playing" | "complete" | "unavailable";

export interface SolanaPublicKey {
  toString(): string;
}

export interface SolanaWalletProvider {
  isConnected?: boolean;
  publicKey?: SolanaPublicKey | null;
  connect(options?: { onlyIfTrusted?: boolean }): Promise<{ publicKey: SolanaPublicKey }>;
  disconnect(): Promise<void>;
  on?(event: "connect" | "disconnect" | "accountChanged", listener: (publicKey?: SolanaPublicKey | null) => void): void;
  removeListener?(event: "connect" | "disconnect" | "accountChanged", listener: (publicKey?: SolanaPublicKey | null) => void): void;
}

declare global {
  interface Window {
    solana?: SolanaWalletProvider;
  }
}

export interface MarketState {
  market: string;
  fair_value: number | null;
  best_bid: number | null;
  best_ask: number | null;
  bid_size: number;
  ask_size: number;
  position: number;
  cash_micros: number;
  fees_micros: number;
  pnl_micros: number;
  phase: string;
  score_home: number;
  score_away: number;
  danger: boolean;
  suspended: boolean;
  source_seq: number;
  venue_seq: number;
}

export interface OrderIntent {
  id: string;
  market: string;
  kind: OrderIntentKind;
  side: Side;
  limit_price: number;
  quantity: number;
  expected_edge_micros: number;
}

export interface TradeEvent {
  order_id: string;
  market: string;
  kind: OrderIntentKind;
  action: TradeAction;
  timestamp: string;
  price: number;
  edge_micros: number;
  quantity: number;
  realized_pnl_micros: number;
}

export interface PositionLifecycle {
  status: PositionStatus;
  entry_price: number | null;
  exit_price: number | null;
  entry_time: string | null;
  exit_time: string | null;
  holding_time_ns: number;
  realized_pnl_micros: number;
}

export interface Decision {
  id: string;
  at: string;
  market: string;
  action: string;
  reason: string;
  intent: OrderIntent | null;
  compute_latency_ns: number;
}

export interface Fill {
  order_id: string;
  market: string;
  side: Side;
  price: number;
  quantity: number;
  fee_micros: number;
  acknowledged_time_ns: number;
}

export interface NextOrderRequirement {
  side: Side;
  quantity: number;
  price: number;
  collateral_micros: number;
  fee_micros: number;
  required_funds_micros: number;
  decision_status: string;
}

export interface RiskCapacity {
  position_limit: number;
  notional_limit_micros: number;
  effective_position_limit: number;
  remaining_contracts: number;
  limiting_gate: string;
}

export interface Snapshot {
  run_id: string;
  mode: "simulated" | "live";
  running: boolean;
  killed: boolean;
  feed_status: Record<string, FeedStatus>;
  markets: MarketState[];
  decisions: Decision[];
  fills: Fill[];
  trades: TradeEvent[];
  position_lifecycle: PositionLifecycle;
  latency: {
    samples: number;
    p50_us: number;
    p95_us: number;
    p99_us: number;
    max_us: number;
  };
  next_order_requirement?: NextOrderRequirement | null;
  risk_capacity?: RiskCapacity;
  processed_events: number;
  rejected_orders: number;
  last_update: string;
}

export interface ReplayState {
  status: ReplayStatus;
  event_index: number;
  total_events: number;
  speed: number;
  journal: string;
}

export interface MarketDisplay {
  event: string;
  contract: string;
  period: string;
  starts_at_ms: number | null;
}

export interface SessionState {
  environment: Environment;
  run_mode: RunMode;
  execution: "simulated";
  live_available: boolean;
  mapping_status: MappingStatus;
  replay: ReplayState;
  market: MarketDisplay | null;
}

export interface FeedModeState {
  mode: FeedMode;
  live_available: boolean;
  mapping_status: MappingStatus;
}
