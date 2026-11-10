// The dashboard's API: ClickHouse's own fixed endpoints (deploy/clickhouse/api.xml), through Caddy.

export interface Kpis {
  payments: number; approved: number; declined_fraud: number; declined_other: number;
  captured_minor: number; fraud_blocked_minor: number; unscored: number; refunded_minor: number;
  payouts: number; payouts_settled: number; payouts_settled_minor: number; payouts_failed: number; in_progress: number;
}
export interface Throughput { t: string; approved: number; declined_fraud: number; declined_other: number; in_progress: number; captured_minor: number }
export interface ScoreBucket { bucket: number; approved: number; declined: number }
export interface Reason { reason: string; declines: number }
export interface Merchant { merchant: string; payments: number; captured_minor: number; declined_share: number; mean_score: number | null }
export interface InFlight { t: string; kind: "holds" | "clearing"; balance: number }
export interface PayoutRow { state: string; reason: string; payouts: number; amount_minor: number; seconds_p50: number; seconds_p95: number }
export interface Pipeline { topic: string; events: number; lag_ms_p50: number; lag_ms_p95: number; last_event_s: number }
export interface Books { kind: string; currency: string; accounts: number; balance_minor: number }

// ClickHouse writes 64-bit integers as strings in JSON: numbers they are.
function numbers<T>(rows: Record<string, unknown>[]): T[] {
  return rows.map((r) => {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(r)) out[k] = typeof v === "string" && /^-?\d+(\.\d+)?$/.test(v) ? Number(v) : v;
    return out as T;
  });
}

async function get<T>(name: string, minutes: number): Promise<T[]> {
  const res = await fetch(`/api/${name}?param_minutes=${minutes}`, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${name}: HTTP ${res.status}`);
  const body = (await res.json()) as { data: Record<string, unknown>[] };
  return numbers<T>(body.data);
}

export async function load(minutes: number) {
  const [kpis, throughput, scores, reasons, merchants, inFlight, payouts, pipeline, books] = await Promise.all([
    get<Kpis>("kpis", minutes),
    get<Throughput>("throughput", minutes),
    get<ScoreBucket>("fraud", minutes),
    get<Reason>("fraud_reasons", minutes),
    get<Merchant>("merchants", minutes),
    get<InFlight>("in_flight", minutes),
    get<PayoutRow>("payouts", minutes),
    get<Pipeline>("pipeline", minutes),
    get<Books>("books", minutes),
  ]);
  return { kpis: kpis[0], throughput, scores, reasons, merchants, inFlight, payouts, pipeline, books };
}

export type Snapshot = Awaited<ReturnType<typeof load>>;
