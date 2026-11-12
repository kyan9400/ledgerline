<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { load, type Snapshot } from "./api";
import Card from "./components/Card.vue";
import Chart, { type Series } from "./components/Chart.vue";
import { REASONS, clock, ms, n, pct, rub, rubShort, secs, shortCode } from "./format";

const WINDOWS = [
  { label: "15 min", minutes: 15 },
  { label: "1 hour", minutes: 60 },
  { label: "6 hours", minutes: 360 },
  { label: "24 hours", minutes: 1440 },
];
const minutes = ref(Number(new URLSearchParams(location.search).get("minutes")) || 60);
const data = ref<Snapshot | null>(null);
const error = ref<string | null>(null);
const updated = ref<Date | null>(null);
const now = ref(Date.now());
let timer: number | undefined;
let ticker: number | undefined;

async function refresh() {
  try {
    data.value = await load(minutes.value);
    error.value = null;
    updated.value = new Date();
  } catch (e) {
    error.value = (e as Error).message;
  }
}
onMounted(() => {
  refresh();
  timer = window.setInterval(refresh, 5000);
  ticker = window.setInterval(() => (now.value = Date.now()), 1000);
});
onBeforeUnmount(() => { clearInterval(timer); clearInterval(ticker); });
watch(minutes, (m) => {
  history.replaceState(null, "", `?minutes=${m}`);
  refresh();
});

const k = computed(() => data.value?.kpis);
const approvalRate = computed(() => (k.value && k.value.payments ? k.value.approved / k.value.payments : 0));
const freshness = computed(() => data.value?.pipeline.find((p) => p.topic === "payments.events"));
const age = computed(() => (updated.value ? Math.round((now.value - updated.value.getTime()) / 1000) : null));

// Payments by outcome, per interval.
const outcomes = computed(() => {
  const rows = data.value?.throughput ?? [];
  return {
    labels: rows.map((r) => clock(r.t)),
    series: [
      { name: "Approved", role: "series-1", values: rows.map((r) => r.approved) },
      { name: "Declined: fraud suspected", role: "series-2", values: rows.map((r) => r.declined_fraud) },
      { name: "Declined: other", role: "series-3", values: rows.map((r) => r.declined_other) },
    ] as Series[],
  };
});
const captured = computed(() => {
  const rows = data.value?.throughput ?? [];
  return { labels: rows.map((r) => clock(r.t)), series: [{ name: "Captured", role: "series-1", values: rows.map((r) => r.captured_minor) }] as Series[] };
});

// Fraud scores, in 0.05 buckets, by the scorer's decision. Nearly every payment scores under 0.05; drawn,
// that one bar would flatten the rest, so the chart starts at 0.05 and the note says how many are below.
const lowScores = computed(() => (data.value?.scores ?? []).filter((r) => r.bucket < 0.05).reduce((s, r) => s + r.approved + r.declined, 0));
const scores = computed(() => {
  const rows = (data.value?.scores ?? []).filter((r) => r.bucket >= 0.05);
  return {
    labels: rows.map((r) => r.bucket.toFixed(2)),
    series: [
      { name: "Approved", role: "series-1", values: rows.map((r) => r.approved) },
      { name: "Declined", role: "series-2", values: rows.map((r) => r.declined) },
    ] as Series[],
  };
});
const reasons = computed(() => {
  const rows = data.value?.reasons ?? [];
  return { labels: rows.map((r) => REASONS[r.reason] ?? r.reason), series: [{ name: "Declines", role: "series-2", values: rows.map((r) => r.declines) }] as Series[] };
});

// Money in flight: holds and clearing, carried forward across minutes without postings.
const inFlight = computed(() => {
  const rows = data.value?.inFlight ?? [];
  const times = [...new Set(rows.map((r) => r.t))].sort();
  const last: Record<string, number> = { holds: 0, clearing: 0 };
  const by: Record<string, number[]> = { holds: [], clearing: [] };
  for (const t of times) {
    for (const kind of ["holds", "clearing"] as const) {
      const row = rows.find((r) => r.t === t && r.kind === kind);
      if (row) last[kind] = row.balance;
      by[kind].push(last[kind]);
    }
  }
  return {
    labels: times.map((t) => clock(t)),
    series: [
      { name: "Card holds", role: "series-1", values: by.holds },
      { name: "Payouts on their way", role: "series-2", values: by.clearing },
    ] as Series[],
  };
});
const booksTotal = computed(() => (data.value?.books ?? []).reduce((s, b) => s + b.balance_minor, 0));
const count = (v: number) => n(v);
// "bank_rejected:account_closed" -> "bank rejected: account closed"
const reason = (r: string) => r.replaceAll("_", " ").replaceAll(":", ": ");
</script>

<template>
  <div class="page">
    <header class="top">
      <div class="brand">
        <h1>Ledgerline <span>· Live</span></h1>
        <p>Card payments, fraud, payouts and the books, from ClickHouse fed by Kafka.</p>
      </div>
      <div class="controls">
        <nav class="links" aria-label="More">
          <a href="/grafana/d/overview">Grafana: SLOs and alerts</a>
          <a href="https://github.com/kyan9400/ledgerline">Source</a>
        </nav>
        <div class="seg" role="radiogroup" aria-label="Time window">
          <button v-for="w in WINDOWS" :key="w.minutes" role="radio" :aria-checked="minutes === w.minutes"
                  :class="{ on: minutes === w.minutes }" @click="minutes = w.minutes">{{ w.label }}</button>
        </div>
        <p class="status num" :class="{ bad: error }">
          <span class="dot" aria-hidden="true"></span>
          <template v-if="error">Can't reach the analytics: {{ error }}</template>
          <template v-else-if="age !== null">Updated {{ age }} s ago<template v-if="freshness && freshness.events"> · events arrive in {{ ms(freshness.lag_ms_p50) }}</template></template>
          <template v-else>Loading…</template>
        </p>
      </div>
    </header>

    <section v-if="k" class="kpis" aria-label="Headline numbers">
      <div class="kpi"><h3>Card payments</h3><p class="big num">{{ n(k.payments) }}</p><p class="sub num">{{ pct(approvalRate) }} approved</p></div>
      <div class="kpi"><h3>Captured</h3><p class="big num">{{ rubShort(k.captured_minor) }}</p><p class="sub num">{{ rubShort(k.refunded_minor) }} refunded</p></div>
      <div class="kpi"><h3>Fraud suspected, declined</h3><p class="big num">{{ n(k.declined_fraud) }}</p><p class="sub num">{{ rubShort(k.fraud_blocked_minor) }} kept from thieves<template v-if="k.unscored"> · {{ n(k.unscored) }} unscored</template></p></div>
      <div class="kpi"><h3>Payouts settled</h3><p class="big num">{{ n(k.payouts_settled) }}</p><p class="sub num">{{ rubShort(k.payouts_settled_minor) }} · {{ n(k.payouts_failed) }} returned or refused</p></div>
      <div class="kpi"><h3>In progress now</h3><p class="big num">{{ n(k.in_progress) }}</p><p class="sub">steps the worker is finishing</p></div>
    </section>

    <main v-if="data" class="grid">
      <Card class="wide" title="Card payments, by outcome" note="Each authorisation, by what became of it" :legend="outcomes.series" table>
        <Chart kind="bar" stacked :labels="outcomes.labels" :series="outcomes.series" :format="count" label="Card payments per interval by outcome" :height="240" />
        <template #table>
          <table><thead><tr><th>From</th><th>Approved</th><th>Fraud suspected</th><th>Declined, other</th></tr></thead>
            <tbody><tr v-for="r in data.throughput" :key="r.t"><td>{{ clock(r.t) }}</td><td class="num">{{ n(r.approved) }}</td><td class="num">{{ n(r.declined_fraud) }}</td><td class="num">{{ n(r.declined_other) }}</td></tr></tbody></table>
        </template>
      </Card>

      <Card title="Money captured" note="By when the payment was made" table>
        <Chart kind="line" :labels="captured.labels" :series="captured.series" :format="rubShort" label="Money captured per interval" />
        <template #table>
          <table><thead><tr><th>From</th><th>Captured</th></tr></thead>
            <tbody><tr v-for="r in data.throughput" :key="r.t"><td>{{ clock(r.t) }}</td><td class="num">{{ rub(r.captured_minor) }}</td></tr></tbody></table>
        </template>
      </Card>

      <Card title="Money in flight" note="Held for card payments, and on its way to other banks" :legend="inFlight.series" table>
        <Chart kind="line" :labels="inFlight.labels" :series="inFlight.series" :format="rubShort" label="Money in the holds and clearing accounts over time" />
        <template #table>
          <table><thead><tr><th>At</th><th>Card holds</th><th>Payouts on their way</th></tr></thead>
            <tbody><tr v-for="(t, i) in inFlight.labels" :key="t"><td>{{ t }}</td><td class="num">{{ rub(inFlight.series[0].values[i]) }}</td><td class="num">{{ rub(inFlight.series[1].values[i]) }}</td></tr></tbody></table>
        </template>
      </Card>

      <Card title="Fraud scores" :note="`Scored 0.05 and up, by the model's decision; ${n(lowScores)} more scored below 0.05`" :legend="scores.series" table>
        <Chart kind="bar" stacked :labels="scores.labels" :series="scores.series" :format="count" label="Distribution of fraud scores by decision" />
        <template #table>
          <table><thead><tr><th>Score from</th><th>Approved</th><th>Declined</th></tr></thead>
            <tbody><tr v-for="r in data.scores" :key="r.bucket"><td class="num">{{ r.bucket.toFixed(2) }}</td><td class="num">{{ n(r.approved) }}</td><td class="num">{{ n(r.declined) }}</td></tr></tbody></table>
        </template>
      </Card>

      <Card title="Why they were declined" note="The model's strongest reasons, per fraud decline" table>
        <Chart v-if="data.reasons.length" kind="bar" horizontal :labels="reasons.labels" :series="reasons.series" :format="count" label="Top reasons for fraud declines" />
        <p v-else class="empty">No fraud declines in this window.</p>
        <template #table>
          <table><thead><tr><th>Reason</th><th>Declines</th></tr></thead>
            <tbody><tr v-for="r in data.reasons" :key="r.reason"><td>{{ REASONS[r.reason] ?? r.reason }}</td><td class="num">{{ n(r.declines) }}</td></tr></tbody></table>
        </template>
      </Card>

      <Card title="Payouts to other banks" note="How each ended, and how long it took">
        <table v-if="data.payouts.length">
          <thead><tr><th>Outcome</th><th>Payouts</th><th>Amount</th><th>Took, p50</th><th>p95</th></tr></thead>
          <tbody><tr v-for="p in data.payouts" :key="p.state + p.reason">
            <td>{{ p.state }}<span v-if="p.reason !== '-'" class="muted"> · {{ reason(p.reason) }}</span></td>
            <td class="num">{{ n(p.payouts) }}</td><td class="num">{{ rub(p.amount_minor) }}</td><td class="num">{{ secs(p.seconds_p50) }}</td><td class="num">{{ secs(p.seconds_p95) }}</td>
          </tr></tbody>
        </table>
        <p v-else class="empty">No payouts in this window.</p>
      </Card>

      <Card title="Top merchants" note="By money captured">
        <table>
          <thead><tr><th>Merchant</th><th>Payments</th><th>Captured</th><th>Declined</th><th>Mean score</th></tr></thead>
          <tbody><tr v-for="m in data.merchants" :key="m.merchant">
            <td>{{ shortCode(m.merchant) }}</td><td class="num">{{ n(m.payments) }}</td><td class="num">{{ rub(m.captured_minor) }}</td>
            <td class="num">{{ pct(m.declined_share) }}</td><td class="num">{{ m.mean_score == null ? "–" : m.mean_score.toFixed(3) }}</td>
          </tr></tbody>
        </table>
      </Card>

      <Card title="The books" note="Every account, from ClickHouse alone: it all sums to zero">
        <table>
          <thead><tr><th>Accounts</th><th>Count</th><th>Balance</th></tr></thead>
          <tbody>
            <tr v-for="b in data.books" :key="b.kind + b.currency"><td>{{ b.kind }}</td><td class="num">{{ n(b.accounts) }}</td><td class="num">{{ rub(b.balance_minor) }}</td></tr>
            <tr class="total"><td>All</td><td></td><td class="num" :class="booksTotal === 0 ? 'ok' : 'bad'">{{ rub(booksTotal) }} <span>{{ booksTotal === 0 ? "✓ balanced" : "✗ not balanced" }}</span></td></tr>
          </tbody>
        </table>
      </Card>

      <Card title="The pipeline" note="From an event's commit in its service to its row in ClickHouse">
        <table>
          <thead><tr><th>Topic</th><th>Events</th><th>Lag p50</th><th>p95</th><th>Last event</th></tr></thead>
          <tbody><tr v-for="p in data.pipeline" :key="p.topic">
            <td>{{ p.topic }}</td><td class="num">{{ n(p.events) }}</td>
            <td class="num">{{ p.events ? ms(p.lag_ms_p50) : "–" }}</td><td class="num">{{ p.events ? ms(p.lag_ms_p95) : "–" }}</td>
            <td class="num">{{ p.events ? secs(p.last_event_s) + " ago" : "–" }}</td>
          </tr></tbody>
        </table>
      </Card>
    </main>
    <p v-else-if="!error" class="empty">Loading…</p>

    <footer>Ledgerline · amounts in roubles · read-only, refreshed every 5 s</footer>
  </div>
</template>

<style scoped>
.page { max-width: 1280px; margin: 0 auto; padding: 24px 24px 32px; }
.top { display: flex; justify-content: space-between; align-items: flex-end; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }
h1 { font-size: 22px; margin: 0; letter-spacing: -0.01em; }
h1 span { color: var(--ink-2); font-weight: 500; }
.brand p { margin: 4px 0 0; color: var(--ink-2); }
.controls { display: flex; flex-direction: column; align-items: flex-end; gap: 6px; }
.links { display: flex; gap: 14px; font-size: 13px; }
.links a { color: var(--ink-2); text-decoration: none; border-bottom: 1px solid var(--ring); }
.links a:hover { color: var(--ink); }
.seg { display: inline-flex; border: 1px solid var(--ring); border-radius: 8px; padding: 2px; background: var(--surface); }
.seg button { border: 0; background: transparent; color: var(--ink-2); padding: 5px 12px; border-radius: 6px; cursor: pointer; font-size: 13px; }
.seg button.on { background: var(--page); color: var(--ink); font-weight: 600; box-shadow: 0 0 0 1px var(--ring); }
.status { margin: 0; color: var(--ink-2); font-size: 12.5px; display: flex; align-items: center; gap: 6px; }
.status .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--good); }
.status.bad { color: var(--critical); }
.status.bad .dot { background: var(--critical); }

.kpis { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 12px; margin-bottom: 12px; }
.kpi { background: var(--surface); border: 1px solid var(--ring); border-radius: 10px; padding: 14px 16px; min-width: 0; }
.kpi h3 { margin: 0; font-size: 12.5px; font-weight: 500; color: var(--ink-2); }
.big { margin: 6px 0 2px; font-size: 26px; font-weight: 650; letter-spacing: -0.02em; }
.sub { margin: 0; color: var(--ink-2); font-size: 12.5px; }

.grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
.grid .wide { grid-column: span 2; }

table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; font-weight: 500; color: var(--muted); padding: 4px 8px 6px 0; border-bottom: 1px solid var(--grid); white-space: nowrap; }
td { padding: 6px 8px 6px 0; border-bottom: 1px solid var(--grid); }
th:not(:first-child), td.num { text-align: right; }
td.num { white-space: nowrap; }
tbody tr:last-child td { border-bottom: 0; }
.muted { color: var(--muted); }
.total td { font-weight: 600; border-top: 1px solid var(--axis); }
.total span { font-weight: 500; font-size: 12px; margin-left: 4px; }
.ok, .ok span { color: var(--good-ink); }
.bad { color: var(--critical); }
.empty { color: var(--muted); margin: 24px 0; text-align: center; }
footer { margin-top: 20px; color: var(--muted); font-size: 12px; text-align: center; }

@media (max-width: 1100px) {
  .kpis { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 720px) {
  .page { padding: 16px; }
  .kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .grid { grid-template-columns: minmax(0, 1fr); }
  .grid .wide { grid-column: auto; }
  .controls { align-items: flex-start; }
}
</style>
