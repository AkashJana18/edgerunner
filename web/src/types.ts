export type FeedStatus = "connecting" | "live" | "stale" | "disconnected";
export type FeedMode = "inactive" | "live";
export type MappingStatus = "unavailable" | "discovering" | "ready";
export type Side = "bid" | "ask";
export type Environment = "devnet" | "mainnet";
export type RunMode = "live" | "replay";
export type ReplayStatus = "paused" | "playing" | "complete" | "unavailable";

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
  side: Side;
  limit_price: number;
  quantity: number;
  expected_edge_micros: number;
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

export interface Snapshot {
  run_id: string;
  mode: "simulated" | "live";
  running: boolean;
  killed: boolean;
  feed_status: Record<string, FeedStatus>;
  markets: MarketState[];
  decisions: Decision[];
  fills: Fill[];
  latency: {
    samples: number;
    p50_us: number;
    p95_us: number;
    p99_us: number;
    max_us: number;
  };
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
