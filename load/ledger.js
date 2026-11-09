// Load test of the ledger: transfers at rising rates, spread over many accounts, then all into a few
// (every transfer to a merchant locks that merchant's row: the worst case for contention).
//
//   docker run --rm --network ledgerline_internal -v $PWD/load:/load -w /load grafana/k6:2.3.0 \
//     run -e BASE=http://ledger:8080 ledger.js
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE || "http://ledger:8080";
const RATES = (__ENV.RATES || "100,250,500,1000").split(",").map(Number);
const STEP_S = Number(__ENV.STEP_S || 60);
const HOT_RATE = Number(__ENV.HOT_RATE || 250);
const ACCOUNTS = 1000;
const MERCHANTS = 5;

const refused = new Counter("refused_insufficient_funds");
const done = new Counter("transfers_done");
const shed = new Counter("shed_503");
// Shedding (503) and a refusal for funds (409) are answers, not failures: "failed" counts only the rest.
http.setResponseCallback(http.expectedStatuses(201, 409, 503));
const scenarios = {};
const thresholds = {};
RATES.forEach((rate, i) => {
  scenarios[`spread_${rate}`] = {
    executor: "constant-arrival-rate", exec: "spread", rate, timeUnit: "1s", duration: `${STEP_S}s`,
    startTime: `${i * STEP_S}s`, preAllocatedVUs: Math.ceil(rate / 4), maxVUs: rate * 2,
  };
  thresholds[`http_req_duration{scenario:spread_${rate}}`] = ["p(95)<250"];
  thresholds[`http_req_failed{scenario:spread_${rate}}`] = ["rate<0.01"];
  thresholds[`http_reqs{scenario:spread_${rate}}`] = [];
  thresholds[`transfers_done{scenario:spread_${rate}}`] = [];
  thresholds[`shed_503{scenario:spread_${rate}}`] = [];
});
scenarios.hot = {
  executor: "constant-arrival-rate", exec: "hot", rate: HOT_RATE, timeUnit: "1s", duration: `${STEP_S}s`,
  startTime: `${RATES.length * STEP_S}s`, preAllocatedVUs: HOT_RATE, maxVUs: HOT_RATE * 2,
};
thresholds["http_req_duration{scenario:hot}"] = ["p(95)<250"];
thresholds["http_req_failed{scenario:hot}"] = ["rate<0.01"];
thresholds["http_reqs{scenario:hot}"] = [];
thresholds["transfers_done{scenario:hot}"] = [];
thresholds["shed_503{scenario:hot}"] = [];

export const options = {
  scenarios,
  thresholds,
  setupTimeout: "300s",
  summaryTrendStats: ["med", "p(95)", "p(99)", "max"],
  discardResponseBodies: false,
};

const run = `${Date.now()}`;
function post(path, body, key) {
  return http.post(`${BASE}${path}`, JSON.stringify(body), {
    headers: { "Content-Type": "application/json", "Idempotency-Key": key },
    tags: { name: path.startsWith("/v1/transfers") ? "transfer" : "setup" },
  });
}

export function setup() {
  const bank = post("/v1/accounts", { code: `load:${run}:bank`, currency: "RUB", kind: "system", allowNegative: true }, `${run}-bank`).json("id");
  const ids = [];
  for (let i = 0; i < ACCOUNTS; i++) {
    const id = post("/v1/accounts", { code: `load:${run}:c${i}`, currency: "RUB", kind: "customer" }, `${run}-c${i}`).json("id");
    post("/v1/transactions", { entries: [{ account: bank, amount: -10_000_000 }, { account: id, amount: 10_000_000 }] }, `${run}-f${i}`);
    ids.push(id);
  }
  const merchants = [];
  for (let i = 0; i < MERCHANTS; i++) {
    merchants.push(post("/v1/accounts", { code: `load:${run}:m${i}`, currency: "RUB", kind: "merchant" }, `${run}-m${i}`).json("id"));
  }
  return { ids, merchants };
}

function transfer(from, to) {
  const res = post("/v1/transfers", { from, to, amount: 1 + Math.floor(Math.random() * 5000) }, `${run}-${__VU}-${__ITER}-${Math.random()}`);
  if (res.status === 201) done.add(1);
  else if (res.status === 409) refused.add(1);
  else if (res.status === 503) shed.add(1);
  check(res, { "201, 409 (funds) or 503 (shed)": (r) => [201, 409, 503].includes(r.status) });
}

export function spread(data) {
  const a = Math.floor(Math.random() * ACCOUNTS);
  let b = Math.floor(Math.random() * ACCOUNTS);
  if (b === a) b = (a + 1) % ACCOUNTS;
  transfer(data.ids[a], data.ids[b]);
}

export function hot(data) {
  transfer(data.ids[Math.floor(Math.random() * ACCOUNTS)], data.merchants[Math.floor(Math.random() * MERCHANTS)]);
}

export function handleSummary(data) {
  const m = (name, sc) => data.metrics[`${name}{scenario:${sc}}`]?.values ?? {};
  const row = (sc, target) => ({
    scenario: sc,
    target_rps: target,
    requested_rps: (m("http_reqs", sc).count ?? 0) / STEP_S,
    done_rps: (m("transfers_done", sc).count ?? 0) / STEP_S,
    shed_share: (m("shed_503", sc).count ?? 0) / Math.max(1, m("http_reqs", sc).count ?? 0),
    // Times of all answers, the fast 503s included.
    p50_ms: m("http_req_duration", sc).med,
    p95_ms: m("http_req_duration", sc)["p(95)"],
    p99_ms: m("http_req_duration", sc)["p(99)"],
    failed: m("http_req_failed", sc).rate,
  });
  const out = {
    steps: [...RATES.map((r) => row(`spread_${r}`, r)), row("hot", HOT_RATE)],
    refused_insufficient_funds: data.metrics.refused_insufficient_funds?.values.count ?? 0,
    dropped_iterations: data.metrics.dropped_iterations?.values.count ?? 0,
  };
  return { stdout: JSON.stringify(out, null, 1) + "\n", "/load/summary.json": JSON.stringify(out, null, 1) };
}
