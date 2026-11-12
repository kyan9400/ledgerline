<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { load, type Snapshot } from "./api";
import Card from "./components/Card.vue";
import Chart, { type Series } from "./components/Chart.vue";
import Icon from "./components/Icon.vue";
import { REASONS, clock, ms, n, pct, rub, rubShort, secs, shortCode } from "./format";
import { theme } from "./theme";

const WINDOWS = [
  { label: "15m", title: "15 minutes", minutes: 15 },
  { label: "1h", title: "1 hour", minutes: 60 },
  { label: "6h", title: "6 hours", minutes: 360 },
  { label: "24h", title: "24 hours", minutes: 1440 },
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
const age = computed(() => (updated.value ? Math.max(0, Math.round((now.value - updated.value.getTime()) / 1000)) : null));
const windowTitle = computed(() => WINDOWS.find((w) => w.minutes === minutes.value)?.title ?? `${minutes.value} minutes`);

const rows = computed(() => data.value?.throughput ?? []);
const labels = computed(() => rows.value.map((r) => clock(r.t)));

// Payments by outcome, per interval.
const outcomes = computed(() => ({
  labels: labels.value,
  series: [
    { name: "Approved", role: "series-1", values: rows.value.map((r) => r.approved) },
    { name: "Declined: fraud suspected", role: "series-2", values: rows.value.map((r) => r.declined_fraud) },
    { name: "Declined: other", role: "series-3", values: rows.value.map((r) => r.declined_other) },
  ] as Series[],
}));
const captured = computed(() => ({
  labels: labels.value,
  series: [{ name: "Captured", role: "series-1", values: rows.value.map((r) => r.captured_minor) }] as Series[],
}));

// The headline cards' trends.
const spark = (role: string, pick: (r: Snapshot["throughput"][number]) => number): Series[] => [
  { name: "trend", role, values: rows.value.map(pick) },
];
const sparkPayments = computed(() => spark("series-1", (r) => r.approved + r.declined_fraud + r.declined_other));
const sparkCaptured = computed(() => spark("good", (r) => r.captured_minor));
const sparkFraud = computed(() => spark("series-2", (r) => r.declined_fraud));
const sparkProgress = computed(() => spark("series-3", (r) => r.in_progress));

// Fraud scores, in 0.05 buckets, by the scorer's decision. Nearly every payment scores under 0.05; drawn,
// that one bar would flatten the rest, so the chart starts at 0.05 and the note says how many are below.
const lowScores = computed(() => (data.value?.scores ?? []).filter((r) => r.bucket < 0.05).reduce((s, r) => s + r.approved + r.declined, 0));
const scores = computed(() => {
  const rs = (data.value?.scores ?? []).filter((r) => r.bucket >= 0.05);
  return {
    labels: rs.map((r) => r.bucket.toFixed(2)),
    series: [
      { name: "Approved", role: "series-1", values: rs.map((r) => r.approved) },
      { name: "Declined", role: "series-2", values: rs.map((r) => r.declined) },
    ] as Series[],
  };
});
const reasons = computed(() => {
  const rs = data.value?.reasons ?? [];
  return { labels: rs.map((r) => REASONS[r.reason] ?? r.reason), series: [{ name: "Declines", role: "series-2", values: rs.map((r) => r.declines) }] as Series[] };
});

// Money in flight: holds and clearing, carried forward across minutes without postings.
const inFlight = computed(() => {
  const rs = data.value?.inFlight ?? [];
  const times = [...new Set(rs.map((r) => r.t))].sort();
  const last: Record<string, number> = { holds: 0, clearing: 0 };
  const by: Record<string, number[]> = { holds: [], clearing: [] };
  for (const t of times) {
    for (const kind of ["holds", "clearing"] as const) {
      const row = rs.find((r) => r.t === t && r.kind === kind);
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
const booksAccounts = computed(() => (data.value?.books ?? []).reduce((s, b) => s + b.accounts, 0));
const maxMerchant = computed(() => Math.max(1, ...(data.value?.merchants ?? []).map((m) => m.captured_minor)));
const count = (v: number) => n(v);
// "bank_rejected:account_closed" -> "bank rejected: account closed"
const reason = (r: string) => r.replaceAll("_", " ").replaceAll(":", ": ");
const outcomeTone = (state: string) => (state === "settled" ? "good" : state === "returned" || state === "refused" ? "warn" : "neutral");
const lagTone = (lagMs: number) => (lagMs < 2000 ? "good" : lagMs < 10000 ? "warn" : "bad");
</script>

<template>
  <div class="app">
    <header class="bar">
      <div class="bar-in">
        <a class="brand" href="/" aria-label="Ledgerline, live">
          <span class="mark" aria-hidden="true">
            <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
              <path d="M5 4v16h14" /><path d="M9 15V9M13 15v-4M17 15V7" />
            </svg>
          </span>
          <span class="name">Ledgerline</span>
          <span class="live" :class="{ off: error }"><span class="pulse" aria-hidden="true"></span>{{ error ? "Offline" : "Live" }}</span>
        </a>
        <nav class="actions" aria-label="Links and settings">
          <a class="link" href="/grafana/d/overview"><Icon name="chart" :size="16" /><span>SLOs &amp; alerts</span></a>
          <a class="link" href="https://github.com/kyan9400/ledgerline"><Icon name="github" :size="16" /><span>Source</span></a>
          <div class="seg" role="radiogroup" aria-label="Time window">
            <button v-for="w in WINDOWS" :key="w.minutes" role="radio" :aria-checked="minutes === w.minutes" :title="w.title"
                    :class="{ on: minutes === w.minutes }" @click="minutes = w.minutes">{{ w.label }}</button>
          </div>
          <button class="icon-btn" :aria-label="theme === 'dark' ? 'Light theme' : 'Dark theme'" :title="theme === 'dark' ? 'Light theme' : 'Dark theme'"
                  @click="theme = theme === 'dark' ? 'light' : 'dark'">
            <Icon :name="theme === 'dark' ? 'sun' : 'moon'" :size="17" />
          </button>
        </nav>
      </div>
    </header>

    <div class="page">
      <section class="intro">
        <div>
          <h1>Payments, fraud and the books, live</h1>
          <p>A double-entry ledger with card payments and payouts on top, every change streamed through Kafka into ClickHouse. Made-up customers and money.</p>
        </div>
        <ul class="trust" aria-label="Status">
          <li :class="booksTotal === 0 ? 'good' : 'bad'">
            <Icon :name="booksTotal === 0 ? 'check' : 'alert'" :size="16" />
            <span><b>{{ booksTotal === 0 ? "Books balanced" : "Books off" }}</b> {{ n(booksAccounts) }} accounts sum to {{ rub(booksTotal) }}</span>
          </li>
          <li v-if="freshness && freshness.events" :class="lagTone(freshness.lag_ms_p50)">
            <Icon name="zap" :size="16" />
            <span><b>{{ ms(freshness.lag_ms_p50) }}</b> from commit to analytics</span>
          </li>
          <li :class="error ? 'bad' : 'neutral'">
            <Icon name="clock" :size="16" />
            <span v-if="error">Can't reach the analytics: {{ error }}</span>
            <span v-else-if="age !== null"><b>Updated {{ age }} s ago</b> · last {{ windowTitle }}</span>
            <span v-else>Loading…</span>
          </li>
        </ul>
      </section>

      <section v-if="k" class="kpis" aria-label="Headline numbers">
        <article class="kpi">
          <div class="kpi-head"><span class="chip blue"><Icon name="card" :size="16" /></span>Card payments</div>
          <p class="big num">{{ n(k.payments) }}</p>
          <p class="sub"><span class="pill good num">{{ pct(approvalRate) }}</span> approved</p>
          <Chart class="spark" kind="line" spark :labels="labels" :series="sparkPayments" :format="count" label="Card payments over the window" :height="38" />
        </article>
        <article class="kpi">
          <div class="kpi-head"><span class="chip green"><Icon name="banknote" :size="16" /></span>Captured</div>
          <p class="big num">{{ rubShort(k.captured_minor) }}</p>
          <p class="sub num">{{ rubShort(k.refunded_minor) }} refunded</p>
          <Chart class="spark" kind="line" spark :labels="labels" :series="sparkCaptured" :format="rubShort" label="Money captured over the window" :height="38" />
        </article>
        <article class="kpi">
          <div class="kpi-head"><span class="chip orange"><Icon name="shield" :size="16" /></span>Fraud declined</div>
          <p class="big num">{{ n(k.declined_fraud) }}</p>
          <p class="sub num">{{ rubShort(k.fraud_blocked_minor) }} kept from thieves<template v-if="k.unscored"> · {{ n(k.unscored) }} unscored</template></p>
          <Chart class="spark" kind="line" spark :labels="labels" :series="sparkFraud" :format="count" label="Fraud declines over the window" :height="38" />
        </article>
        <article class="kpi">
          <div class="kpi-head"><span class="chip violet"><Icon name="send" :size="16" /></span>Payouts settled</div>
          <p class="big num">{{ n(k.payouts_settled) }}</p>
          <p class="sub num">{{ rubShort(k.payouts_settled_minor) }} · <span :class="{ warn: k.payouts_failed }">{{ n(k.payouts_failed) }} returned or refused</span></p>
          <div class="spark spark-empty" aria-hidden="true"></div>
        </article>
        <article class="kpi">
          <div class="kpi-head"><span class="chip slate"><Icon name="cog" :size="16" /></span>In progress</div>
          <p class="big num">{{ n(k.in_progress) }}</p>
          <p class="sub">steps the worker is finishing</p>
          <Chart class="spark" kind="line" spark :labels="labels" :series="sparkProgress" :format="count" label="Steps in progress over the window" :height="38" />
        </article>
      </section>

      <template v-if="data">
        <h2 class="section">Payments</h2>
        <div class="grid">
          <Card class="wide" icon="card" title="Card payments, by outcome" note="Each authorisation, by what became of it" :legend="outcomes.series" table>
            <Chart kind="bar" stacked :labels="outcomes.labels" :series="outcomes.series" :format="count" label="Card payments per interval by outcome" :height="250" />
            <template #table>
              <table><thead><tr><th>From</th><th>Approved</th><th>Fraud suspected</th><th>Declined, other</th></tr></thead>
                <tbody><tr v-for="r in data.throughput" :key="r.t"><td>{{ clock(r.t) }}</td><td class="num">{{ n(r.approved) }}</td><td class="num">{{ n(r.declined_fraud) }}</td><td class="num">{{ n(r.declined_other) }}</td></tr></tbody></table>
            </template>
          </Card>
          <Card icon="banknote" title="Money captured" note="By when the payment was made" table>
            <Chart kind="line" area :labels="captured.labels" :series="captured.series" :format="rubShort" label="Money captured per interval" :height="250" />
            <template #table>
              <table><thead><tr><th>From</th><th>Captured</th></tr></thead>
                <tbody><tr v-for="r in data.throughput" :key="r.t"><td>{{ clock(r.t) }}</td><td class="num">{{ rub(r.captured_minor) }}</td></tr></tbody></table>
            </template>
          </Card>
        </div>

        <h2 class="section">Risk</h2>
        <div class="grid">
          <Card icon="shield" title="Fraud scores" :note="`Scored 0.05 and up, by decision · ${n(lowScores)} more below 0.05`" :legend="scores.series" table>
            <Chart kind="bar" stacked :labels="scores.labels" :series="scores.series" :format="count" label="Distribution of fraud scores by decision" :height="300" />
            <template #table>
              <table><thead><tr><th>Score from</th><th>Approved</th><th>Declined</th></tr></thead>
                <tbody><tr v-for="r in data.scores" :key="r.bucket"><td class="num">{{ r.bucket.toFixed(2) }}</td><td class="num">{{ n(r.approved) }}</td><td class="num">{{ n(r.declined) }}</td></tr></tbody></table>
            </template>
          </Card>
          <Card icon="alert" title="Why they were declined" note="The model's strongest reasons, per fraud decline" table>
            <Chart v-if="data.reasons.length" kind="bar" horizontal :labels="reasons.labels" :series="reasons.series" :format="count" label="Top reasons for fraud declines" :height="300" />
            <div v-else class="empty"><Icon name="shield" :size="22" /><p>No fraud declines in the last {{ windowTitle }}.</p></div>
            <template #table>
              <table><thead><tr><th>Reason</th><th>Declines</th></tr></thead>
                <tbody><tr v-for="r in data.reasons" :key="r.reason"><td>{{ REASONS[r.reason] ?? r.reason }}</td><td class="num">{{ n(r.declines) }}</td></tr></tbody></table>
            </template>
          </Card>
          <Card icon="store" title="Top merchants" note="By money captured">
            <table v-if="data.merchants.length" class="merchants">
              <thead><tr><th>Merchant</th><th>Payments</th><th>Captured</th><th>Declined</th></tr></thead>
              <tbody><tr v-for="m in data.merchants.slice(0, 7)" :key="m.merchant">
                <td><span class="mono">{{ shortCode(m.merchant) }}</span>
                  <span class="meter" aria-hidden="true"><span :style="{ width: (100 * m.captured_minor / maxMerchant) + '%' }"></span></span></td>
                <td class="num">{{ n(m.payments) }}</td><td class="num">{{ rubShort(m.captured_minor) }}</td>
                <td class="num"><span class="pill num" :class="m.declined_share > 0.05 ? 'bad' : m.declined_share > 0 ? 'warn' : 'neutral'">{{ pct(m.declined_share) }}</span></td>
              </tr></tbody>
            </table>
            <div v-else class="empty"><Icon name="store" :size="22" /><p>No merchant payments in this window.</p></div>
          </Card>
        </div>

        <h2 class="section">Money movement</h2>
        <div class="grid">
          <Card class="wide" icon="banknote" title="Money in flight" note="Held for card payments, and on its way to other banks" :legend="inFlight.series" table>
            <Chart kind="line" area :labels="inFlight.labels" :series="inFlight.series" :format="rubShort" label="Money in the holds and clearing accounts over time" :height="230" />
            <template #table>
              <table><thead><tr><th>At</th><th>Card holds</th><th>Payouts on their way</th></tr></thead>
                <tbody><tr v-for="(t, i) in inFlight.labels" :key="t"><td>{{ t }}</td><td class="num">{{ rub(inFlight.series[0].values[i]) }}</td><td class="num">{{ rub(inFlight.series[1].values[i]) }}</td></tr></tbody></table>
            </template>
          </Card>
          <Card icon="send" title="Payouts to other banks" note="How each ended, and how long it took">
            <table v-if="data.payouts.length">
              <thead><tr><th>Outcome</th><th>Payouts</th><th>Took, p50</th><th>p95</th></tr></thead>
              <tbody><tr v-for="p in data.payouts" :key="p.state + p.reason">
                <td><span class="pill" :class="outcomeTone(p.state)">{{ p.state }}</span><span v-if="p.reason !== '-'" class="muted small"> {{ reason(p.reason) }}</span></td>
                <td class="num">{{ n(p.payouts) }}<span class="muted small"> · {{ rubShort(p.amount_minor) }}</span></td>
                <td class="num">{{ secs(p.seconds_p50) }}</td><td class="num">{{ secs(p.seconds_p95) }}</td>
              </tr></tbody>
            </table>
            <div v-else class="empty"><Icon name="send" :size="22" /><p>No payouts in this window.</p></div>
          </Card>
        </div>

        <h2 class="section">Platform</h2>
        <div class="grid two">
          <Card icon="book" title="The books" note="Every account, from ClickHouse alone: it all sums to zero">
            <div class="balance" :class="booksTotal === 0 ? 'good' : 'bad'">
              <Icon :name="booksTotal === 0 ? 'check' : 'alert'" :size="22" />
              <div><b class="num">{{ rub(booksTotal) }}</b><span>{{ booksTotal === 0 ? "Balanced: money is moved, never made or lost" : "Not balanced" }}</span></div>
            </div>
            <table>
              <thead><tr><th>Accounts</th><th>Count</th><th>Balance</th></tr></thead>
              <tbody><tr v-for="b in data.books" :key="b.kind + b.currency"><td>{{ b.kind }}</td><td class="num">{{ n(b.accounts) }}</td><td class="num">{{ rub(b.balance_minor) }}</td></tr></tbody>
            </table>
          </Card>
          <Card icon="zap" title="The pipeline" note="From an event's commit in its service to its row in ClickHouse">
            <table>
              <thead><tr><th>Topic</th><th>Events</th><th>Lag p50</th><th>p95</th><th>Last event</th></tr></thead>
              <tbody><tr v-for="p in data.pipeline" :key="p.topic">
                <td><span class="dot" :class="p.events ? lagTone(p.lag_ms_p50) : 'neutral'" aria-hidden="true"></span><span class="mono">{{ p.topic }}</span></td>
                <td class="num">{{ n(p.events) }}</td>
                <td class="num">{{ p.events ? ms(p.lag_ms_p50) : "–" }}</td><td class="num">{{ p.events ? ms(p.lag_ms_p95) : "–" }}</td>
                <td class="num">{{ p.events ? secs(p.last_event_s) + " ago" : "–" }}</td>
              </tr></tbody>
            </table>
          </Card>
        </div>
      </template>
      <div v-else-if="!error" class="loading" aria-busy="true">
        <div v-for="i in 6" :key="i" class="skeleton"></div>
      </div>

      <footer>
        <span>Ledgerline · amounts in roubles · read-only, refreshed every 5 s</span>
        <span><a href="https://github.com/kyan9400/ledgerline">github.com/kyan9400/ledgerline</a></span>
      </footer>
    </div>
  </div>
</template>

<style scoped>
/* The bar */
.bar { position: sticky; top: 0; z-index: 20; backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
  background: color-mix(in srgb, var(--page) 78%, transparent); border-bottom: 1px solid var(--ring); }
.bar-in { max-width: 1320px; margin: 0 auto; padding: 10px 28px; display: flex; align-items: center; justify-content: space-between; gap: 16px; }
.brand { display: flex; align-items: center; gap: 10px; text-decoration: none; color: var(--ink); }
.mark { display: grid; place-items: center; width: 30px; height: 30px; border-radius: 9px; color: #fff;
  background: linear-gradient(135deg, #3b82f6, #6366f1); box-shadow: 0 4px 14px rgba(59, 130, 246, 0.35); }
.name { font-weight: 700; font-size: 16px; letter-spacing: -0.01em; }
.live { display: inline-flex; align-items: center; gap: 6px; padding: 2px 9px 2px 8px; border-radius: 999px; font-size: 11.5px;
  font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase; color: var(--good-ink); background: var(--good-soft); border: 1px solid var(--ring); }
.live.off { color: var(--critical); background: var(--critical-soft); }
.pulse { width: 7px; height: 7px; border-radius: 50%; background: currentColor; box-shadow: 0 0 0 0 currentColor; animation: pulse 2s infinite; }
@keyframes pulse { 0% { box-shadow: 0 0 0 0 color-mix(in srgb, currentColor 55%, transparent); } 70% { box-shadow: 0 0 0 7px transparent; } 100% { box-shadow: 0 0 0 0 transparent; } }
.actions { display: flex; align-items: center; gap: 8px; }
.link { display: inline-flex; align-items: center; gap: 7px; padding: 7px 11px; border-radius: 9px; color: var(--ink-2);
  text-decoration: none; font-size: 13px; transition: color 0.15s, background 0.15s; }
.link:hover { color: var(--ink); background: var(--raise); }
.seg { display: inline-flex; padding: 3px; border-radius: 10px; border: 1px solid var(--ring); background: var(--surface); }
.seg button { min-width: 40px; border: 0; background: transparent; color: var(--muted); padding: 5px 10px; border-radius: 7px; cursor: pointer;
  font-size: 12.5px; font-weight: 500; transition: color 0.15s, background 0.15s; }
.seg button:hover { color: var(--ink); }
.seg button.on { background: var(--accent); color: #fff; font-weight: 600; }
.icon-btn { display: grid; place-items: center; width: 36px; height: 36px; border-radius: 10px; border: 1px solid var(--ring);
  background: var(--surface); color: var(--ink-2); cursor: pointer; transition: color 0.15s, border-color 0.15s; }
.icon-btn:hover { color: var(--ink); border-color: var(--ring-strong); }

/* The page */
.page { max-width: 1320px; margin: 0 auto; padding: 28px 28px 36px; }
.intro { display: flex; justify-content: space-between; align-items: flex-end; gap: 24px; flex-wrap: wrap; margin-bottom: 22px; }
h1 { font-size: 26px; line-height: 1.2; margin: 0; letter-spacing: -0.02em; font-weight: 700; }
.intro p { margin: 6px 0 0; color: var(--ink-2); max-width: 620px; }
.trust { list-style: none; margin: 0; padding: 0; display: flex; flex-wrap: wrap; gap: 8px; }
.trust li { display: inline-flex; align-items: center; gap: 8px; padding: 7px 12px; border-radius: 10px; font-size: 12.5px;
  color: var(--ink-2); background: var(--surface); border: 1px solid var(--ring); }
.trust b { color: var(--ink); font-weight: 600; }
.trust li.good svg { color: var(--good); }
.trust li.warn svg { color: var(--warn); }
.trust li.bad { color: var(--critical); }
.trust li.bad svg { color: var(--critical); }
.trust li.neutral svg { color: var(--muted); }

/* Headline cards */
.kpis { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 14px; margin-bottom: 8px; }
.kpi { position: relative; overflow: hidden; display: flex; flex-direction: column; min-width: 0; padding: 16px 18px 12px;
  border-radius: 14px; border: 1px solid var(--ring); background: linear-gradient(180deg, var(--surface-2), var(--surface)); box-shadow: var(--shadow); }
.kpi-head { display: flex; align-items: center; gap: 10px; color: var(--ink-2); font-size: 13px; font-weight: 500; }
.chip { display: grid; place-items: center; width: 30px; height: 30px; border-radius: 9px; border: 1px solid var(--ring); }
.chip.blue { color: var(--series-1); background: var(--accent-soft); }
.chip.green { color: var(--good); background: var(--good-soft); }
.chip.orange { color: var(--series-2); background: color-mix(in srgb, var(--series-2) 14%, transparent); }
.chip.violet { color: var(--series-3); background: color-mix(in srgb, var(--series-3) 14%, transparent); }
.chip.slate { color: var(--ink-2); background: var(--raise); }
.big { margin: 14px 0 4px; font-size: 30px; line-height: 1.1; font-weight: 700; letter-spacing: -0.025em; }
.sub { margin: 0 0 12px; color: var(--muted); font-size: 12.5px; }
.sub .warn { color: var(--warn); }
.spark { margin: auto -18px -12px; width: calc(100% + 36px); }
.spark-empty { height: 38px; }

/* Sections */
.section { margin: 30px 0 12px; font-size: 12px; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted);
  display: flex; align-items: center; gap: 12px; }
.section::after { content: ""; flex: 1; height: 1px; background: var(--ring); }
.grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
.grid.two { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.grid .wide { grid-column: span 2; }

/* Tables */
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; font-weight: 500; font-size: 11.5px; letter-spacing: 0.03em; text-transform: uppercase; color: var(--muted);
  padding: 0 10px 8px 0; border-bottom: 1px solid var(--ring); white-space: nowrap; }
td { padding: 9px 10px 9px 0; border-bottom: 1px solid var(--grid); color: var(--ink); }
th:not(:first-child), td.num { text-align: right; }
td.num { white-space: nowrap; }
tbody tr { transition: background 0.15s; }
tbody tr:hover { background: color-mix(in srgb, var(--raise) 60%, transparent); }
tbody tr:last-child td { border-bottom: 0; }
.muted { color: var(--muted); }
.small { font-size: 12px; }
.pill { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 12px; font-weight: 600; text-transform: none; }
.pill.good { color: var(--good-ink); background: var(--good-soft); }
.pill.warn { color: var(--warn); background: var(--warn-soft); }
.pill.bad { color: var(--critical); background: var(--critical-soft); }
.pill.neutral { color: var(--ink-2); background: var(--raise); }
.merchants td:first-child { min-width: 110px; }
.meter { display: block; height: 4px; margin-top: 6px; border-radius: 4px; background: var(--raise); overflow: hidden; }
.meter span { display: block; height: 100%; border-radius: 4px; background: linear-gradient(90deg, var(--series-1), color-mix(in srgb, var(--series-1) 60%, #a78bfa)); }
.dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 8px; vertical-align: 1px; }
.dot.good { background: var(--good); box-shadow: 0 0 0 3px var(--good-soft); }
.dot.warn { background: var(--warn); box-shadow: 0 0 0 3px var(--warn-soft); }
.dot.bad { background: var(--critical); box-shadow: 0 0 0 3px var(--critical-soft); }
.dot.neutral { background: var(--muted); }

.balance { display: flex; align-items: center; gap: 14px; padding: 14px 16px; margin-bottom: 14px; border-radius: 12px; border: 1px solid var(--ring); }
.balance.good { background: var(--good-soft); color: var(--good); }
.balance.bad { background: var(--critical-soft); color: var(--critical); }
.balance b { display: block; font-size: 22px; letter-spacing: -0.02em; color: var(--ink); }
.balance span { font-size: 12.5px; color: var(--ink-2); }

.empty { display: grid; place-items: center; gap: 8px; min-height: 180px; color: var(--muted); text-align: center; }
.empty p { margin: 0; font-size: 13px; }
.loading { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin-top: 24px; }
.skeleton { height: 260px; border-radius: 14px; border: 1px solid var(--ring);
  background: linear-gradient(90deg, var(--surface) 0%, var(--raise) 50%, var(--surface) 100%); background-size: 200% 100%; animation: shimmer 1.4s infinite; }
@keyframes shimmer { from { background-position: 200% 0; } to { background-position: -200% 0; } }

footer { margin-top: 34px; padding-top: 18px; border-top: 1px solid var(--ring); display: flex; justify-content: space-between; gap: 12px;
  flex-wrap: wrap; color: var(--muted); font-size: 12.5px; }
footer a { color: var(--ink-2); text-decoration: none; }
footer a:hover { color: var(--ink); }

@media (max-width: 1180px) {
  .kpis { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 760px) {
  .bar-in, .page { padding-left: 16px; padding-right: 16px; }
  .bar-in { flex-wrap: wrap; row-gap: 8px; }
  .actions { width: 100%; justify-content: space-between; }
  .actions .seg { margin-left: auto; }
  .link { padding: 7px 8px; }
  .link span { display: none; }
  .kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .grid, .grid.two, .loading { grid-template-columns: minmax(0, 1fr); }
  .grid .wide { grid-column: auto; }
  h1 { font-size: 22px; }
  .big { font-size: 26px; }
}
@media (max-width: 420px) {
  .kpis { grid-template-columns: minmax(0, 1fr); }
  .seg button { min-width: 34px; padding: 5px 7px; }
}
</style>
