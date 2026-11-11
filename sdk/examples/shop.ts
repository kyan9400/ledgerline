// A merchant, end to end: it takes card payments through the SDK, listens for webhooks and watches its
// payments' events, then proves it heard about every change, exactly as Ledgerline recorded it.
//
//   node examples/shop.ts     (env: TARGET=payments:9090, ADMIN=http://payments:8080, LEDGER=http://ledger:8080,
//                              PORT=8070, HOOK_URL=http://shop:8070/hooks, SECONDS=60, RATE=4,
//                              FLAKY_SECONDS=30 (the webhook endpoint refuses everything at first), DEADLINE_S=300)
//
// Its setup (a merchant account, and customers with money) uses Ledgerline's internal APIs: in real life
// the merchant gets its keys from onboarding, and customers have their own money.
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { Ledgerline, LedgerlineError, verifyWebhook, type Payment } from "../src/index.ts";

const env = process.env;
const ADMIN = env.ADMIN ?? "http://payments:8080";
const LEDGER = env.LEDGER ?? "http://ledger:8080";
const PORT = Number(env.PORT ?? 8070);
const SECONDS = Number(env.SECONDS ?? 60);
const RATE = Number(env.RATE ?? 4);
const FLAKY_SECONDS = Number(env.FLAKY_SECONDS ?? 30);
const DEADLINE = Date.now() + (SECONDS + Number(env.DEADLINE_S ?? 300)) * 1000;

async function post(url: string, body: unknown, key?: string): Promise<any> {
  for (let i = 0; ; i++) {
    try {
      const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json", ...(key ? { "Idempotency-Key": key } : {}) }, body: JSON.stringify(body) });
      if (r.status < 500) return r.json();
    } catch { /* not up yet */ }
    if (i > 30) throw new Error(`POST ${url}: no answer`);
    await new Promise((r) => setTimeout(r, 1000));
  }
}

// --- setup (simulation only) ---------------------------------------------------------------------
const run = Date.now().toString(36);
const merchant = await post(`${ADMIN}/internal/merchants`, { name: `Example Shop ${run}`, webhookUrl: env.HOOK_URL ?? "http://shop:8070/hooks" });
const bank = (await post(`${LEDGER}/v1/accounts`, { code: `shop-demo:${run}:bank`, currency: "RUB", kind: "system", allowNegative: true }, randomUUID())).id;
const customers: string[] = [];
for (let i = 0; i < 20; i++) {
  const c = (await post(`${LEDGER}/v1/accounts`, { code: `shop-demo:${run}:c${i}`, currency: "RUB", kind: "customer" }, randomUUID())).id;
  await post(`${LEDGER}/v1/transfers`, { from: bank, to: c, amount: 50_000_000 }, randomUUID());
  customers.push(c);
}

// --- webhooks -------------------------------------------------------------------------------------
const started = Date.now();
const hooks = new Map<string, { type: string; version: number }>();
let duplicates = 0;
let refusedWhileDown = 0;
let badSignatures = 0;
const server = createServer((req, res) => {
  const chunks: Buffer[] = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    if (Date.now() - started < FLAKY_SECONDS * 1000) {
      refusedWhileDown++;
      res.writeHead(503).end(); // down for a while: Ledgerline must try again later
      return;
    }
    try {
      const e = verifyWebhook(Buffer.concat(chunks), req.headers["ledgerline-signature"] as string, merchant.webhookSecret);
      if (hooks.has(e.id)) duplicates++;
      else hooks.set(e.id, { type: e.type, version: e.version });
      res.writeHead(204).end();
    } catch {
      badSignatures++;
      res.writeHead(400).end();
    }
  });
});
server.listen(PORT);

// --- the event stream -----------------------------------------------------------------------------
const client = new Ledgerline({ target: env.TARGET ?? "payments:9090", apiKey: merchant.apiKey });
const streamed = new Set<string>();
const stop = new AbortController();
const watching = (async () => {
  for await (const e of client.watch({ signal: stop.signal })) streamed.add(e.id);
})();
await new Promise((r) => setTimeout(r, 2000)); // the stream starts from "now"

// --- orders ---------------------------------------------------------------------------------------
const payments = new Map<string, Payment>();
const refunds = new Map<string, string>(); // refund id -> payment id
const errors = new Map<string, number>();
const note = (e: unknown) => {
  const k = e instanceof LedgerlineError ? e.code ?? `grpc_${e.grpcCode}` : String(e);
  errors.set(k, (errors.get(k) ?? 0) + 1);
};
const pick = <T>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)];

async function order(): Promise<void> {
  const amount = 1_000 + Math.floor(Math.random() * 200_000);
  let p = await client.authorise({ customer: pick(customers), amount, description: "order" }, { idempotencyKey: `order-${randomUUID()}` });
  payments.set(p.id, p);
  while (p.state === "authorising" && Date.now() < DEADLINE) {
    await new Promise((r) => setTimeout(r, 300));
    p = await client.getPayment(p.id);
  }
  if (p.state !== "authorised") return;
  const roll = Math.random();
  if (roll < 0.15) {
    await client.voidPayment(p.id);
    return;
  }
  const c = await client.capture(p.id, roll < 0.3 ? Math.floor(amount / 2) : undefined);
  if (c.state === "captured" && Math.random() < 0.4) {
    const r = await client.refund(p.id, Math.floor(c.captured / 3));
    refunds.set(r.id, p.id);
  }
}

const inFlight: Promise<void>[] = [];
const end = started + SECONDS * 1000;
while (Date.now() < end) {
  inFlight.push(order().catch(note));
  await new Promise((r) => setTimeout(r, 1000 / RATE));
}
await Promise.all(inFlight);

// --- did we hear about everything? ------------------------------------------------------------------
// What Ledgerline has, by the API: every version of every payment and refund is an event.
async function expected(): Promise<Set<string>> {
  const ids = new Set<string>();
  for (const id of payments.keys()) {
    const p = await client.getPayment(id);
    for (let v = 1; v <= p.version; v++) ids.add(`payment-${id}:${v}`);
  }
  for (const rid of refunds.keys()) for (let v = 1; v <= 2; v++) ids.add(`refund-${rid}:${v}`);
  return ids;
}
let want = await expected();
const missing = (have: { has(k: string): boolean }) => [...want].filter((id) => !have.has(id));
while (Date.now() < DEADLINE && (missing(hooks).length || missing(streamed).length)) {
  await new Promise((r) => setTimeout(r, 2000));
  want = await expected();
}
stop.abort();
client.close();
server.close();
await watching.catch(() => {});

const reader = new Ledgerline({ target: env.TARGET ?? "payments:9090", apiKey: merchant.apiKey });
const finals = await Promise.all([...payments.keys()].map((id) => reader.getPayment(id).catch(() => null)));
reader.close();
const report = {
  ok: missing(hooks).length === 0 && missing(streamed).length === 0 && badSignatures === 0,
  orders: payments.size,
  payments_by_state: Object.fromEntries([...finals].reduce((m, p) => m.set(p?.state ?? "?", (m.get(p?.state ?? "?") ?? 0) + 1), new Map<string, number>())),
  refunds: refunds.size,
  errors: Object.fromEntries(errors),
  events_expected: want.size,
  webhooks: { received: hooks.size, missing: missing(hooks).length, duplicates_skipped: duplicates, refused_while_down: refusedWhileDown, bad_signatures: badSignatures },
  stream: { received: [...streamed].filter((id) => want.has(id)).length, missing: missing(streamed).length },
  seconds: Math.round((Date.now() - started) / 1000),
};
console.log(JSON.stringify(report, null, 1));
process.exit(report.ok ? 0 : 1);
