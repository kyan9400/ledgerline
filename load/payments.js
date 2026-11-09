// Load test of the payments service: card payments (authorise, then capture) at rising rates, with
// payouts to the (misbehaving) other bank alongside throughout. Every request is a ledger posting or two
// behind the payments service, so this measures the whole path.
//
//   docker run --rm --network ledgerline_internal -v $PWD/load:/load -w /load grafana/k6:2.3.0 \
//     run -e PAYMENTS=http://payments:8080 -e LEDGER=http://ledger:8080 payments.js
import http from "k6/http";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

const PAYMENTS = __ENV.PAYMENTS || "http://payments:8080";
const LEDGER = __ENV.LEDGER || "http://ledger:8080";
const RATES = (__ENV.RATES || "50,100,150,200").split(",").map(Number);
const STEP_S = Number(__ENV.STEP_S || 60);
const PAYOUT_RATE = Number(__ENV.PAYOUT_RATE || 10);
const CUSTOMERS = 500;
const MERCHANTS = 20;

const authorised = new Counter("payments_authorised");
const captured = new Counter("payments_captured");
const declined = new Counter("payments_declined");
const pending = new Counter("payments_pending_202");
const shed = new Counter("shed_503");
const payouts = new Counter("payouts_accepted");
const authTime = new Trend("authorise_ms", true);
const captureTime = new Trend("capture_ms", true);
http.setResponseCallback(http.expectedStatuses(200, 201, 202, 503));

const scenarios = {};
const thresholds = {};
RATES.forEach((rate, i) => {
  scenarios[`cards_${rate}`] = {
    executor: "constant-arrival-rate", exec: "card", rate, timeUnit: "1s", duration: `${STEP_S}s`,
    startTime: `${i * STEP_S}s`, preAllocatedVUs: rate, maxVUs: rate * 4,
  };
  for (const m of ["authorise_ms", "capture_ms", "payments_authorised", "payments_captured", "shed_503", "payments_pending_202", "http_req_failed"]) {
    thresholds[`${m}{scenario:cards_${rate}}`] = [];
  }
});
scenarios.payouts = {
  executor: "constant-arrival-rate", exec: "payout", rate: PAYOUT_RATE, timeUnit: "1s", duration: `${RATES.length * STEP_S}s`,
  preAllocatedVUs: 20, maxVUs: 100,
};
thresholds["payouts_accepted"] = [];

export const options = { scenarios, thresholds, setupTimeout: "300s", summaryTrendStats: ["med", "p(95)", "p(99)", "max"] };

const run = `${Date.now()}`;
function post(base, path, body, key, name) {
  return http.post(`${base}${path}`, JSON.stringify(body), {
    headers: { "Content-Type": "application/json", "Idempotency-Key": key },
    tags: { name },
  });
}

export function setup() {
  const bank = post(LEDGER, "/v1/accounts", { code: `load:${run}:bank`, currency: "RUB", kind: "system", allowNegative: true }, `${run}-bank`, "setup").json("id");
  const customers = [];
  for (let i = 0; i < CUSTOMERS; i++) {
    const id = post(LEDGER, "/v1/accounts", { code: `load:${run}:c${i}`, currency: "RUB", kind: "customer" }, `${run}-c${i}`, "setup").json("id");
    post(LEDGER, "/v1/transactions", { entries: [{ account: bank, amount: -100_000_000 }, { account: id, amount: 100_000_000 }] }, `${run}-f${i}`, "setup");
    customers.push(id);
  }
  const merchants = [];
  for (let i = 0; i < MERCHANTS; i++) {
    merchants.push(post(LEDGER, "/v1/accounts", { code: `load:${run}:m${i}`, currency: "RUB", kind: "merchant" }, `${run}-m${i}`, "setup").json("id"));
  }
  return { customers, merchants };
}

const pick = (a) => a[Math.floor(Math.random() * a.length)];
const key = () => `${run}-${__VU}-${__ITER}-${Math.random()}`;

export function card(data) {
  const amount = 100 + Math.floor(Math.random() * 50_000);
  const a = post(PAYMENTS, "/v1/payments", { customer: pick(data.customers), merchant: pick(data.merchants), amount }, key(), "authorise");
  authTime.add(a.timings.duration);
  if (a.status === 503) return shed.add(1);
  if (a.status === 202) return pending.add(1);
  if (a.json("state") === "declined") return declined.add(1);
  authorised.add(1);
  // Most are captured, some in part; a few are left to expire or voided.
  const roll = Math.random();
  if (roll < 0.05) {
    post(PAYMENTS, `/v1/payments/${a.json("id")}/void`, {}, key(), "void");
    return;
  }
  const body = roll < 0.25 ? { amount: Math.max(1, Math.floor(amount / 2)) } : {};
  const c = post(PAYMENTS, `/v1/payments/${a.json("id")}/capture`, body, key(), "capture");
  captureTime.add(c.timings.duration);
  if (c.status === 503) return shed.add(1);
  if (c.status === 200) captured.add(1);
  check(c, { "capture: 200, 202 or 503": (r) => [200, 202, 503].includes(r.status) });
}

export function payout(data) {
  const r = post(PAYMENTS, "/v1/payouts", { account: pick(data.customers), amount: 100 + Math.floor(Math.random() * 10_000), bank: "otherbank", beneficiary: `ru-${Math.floor(Math.random() * 1e9)}` }, key(), "payout");
  if (r.status === 202) payouts.add(1);
}

export function handleSummary(data) {
  const m = (name, sc) => data.metrics[`${name}{scenario:${sc}}`]?.values ?? {};
  const steps = RATES.map((rate) => {
    const sc = `cards_${rate}`;
    return {
      scenario: sc,
      target_per_s: rate,
      authorised_per_s: (m("payments_authorised", sc).count ?? 0) / STEP_S,
      captured_per_s: (m("payments_captured", sc).count ?? 0) / STEP_S,
      shed: m("shed_503", sc).count ?? 0,
      pending_202: m("payments_pending_202", sc).count ?? 0,
      authorise_p50_ms: m("authorise_ms", sc).med,
      authorise_p95_ms: m("authorise_ms", sc)["p(95)"],
      authorise_p99_ms: m("authorise_ms", sc)["p(99)"],
      capture_p50_ms: m("capture_ms", sc).med,
      capture_p95_ms: m("capture_ms", sc)["p(95)"],
      failed: m("http_req_failed", sc).rate,
    };
  });
  const out = {
    steps,
    payouts_accepted: data.metrics.payouts_accepted?.values.count ?? 0,
    declined: data.metrics.payments_declined?.values.count ?? 0,
    dropped_iterations: data.metrics.dropped_iterations?.values.count ?? 0,
  };
  return { stdout: JSON.stringify(out, null, 1) + "\n", "/load/summary-payments.json": JSON.stringify(out, null, 1) };
}
