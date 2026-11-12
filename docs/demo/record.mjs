// Records the demo (docs/demo) against the live stack: the analytics dashboard, then Grafana while the
// ledger is stopped for two and a half minutes under load and comes back, then a payment's trace
// through the outage. Frames are screenshots (every 3 s during the outage: a time-lapse); make_video.sh
// turns them into the video and the GIF.
//
//   SSH=shade-vps BASE=https://ledgerline.example OUT=out node record.mjs
//   FROM_PART=3 WINDOW=<from ms>,<to ms> ... node record.mjs     to redo the last parts only
//
// It drives the server over ssh: extra load for ten minutes (the checker's workload and the example
// shop), the ledger stopped and started. Nothing else is changed. Uses the Edge already installed
// (playwright-core, no browser download).
import { chromium } from "playwright-core";
import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";

const BASE = process.env.BASE ?? "https://ledgerline.149-36-1-90.sslip.io";
const SSH = process.env.SSH ?? "shade-vps";
const OUT = process.env.OUT ?? "out";
const C = "cd ~/ledgerline && docker compose -f deploy/compose.yaml -f deploy/chaos.yaml -f deploy/compose.edge.yaml --env-file deploy/.env";
const size = { width: 1440, height: 900 };
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
const remote = (cmd) => execFileSync("ssh", [SSH, cmd], { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });

for (const d of ["frames/1-live", "frames/2-outage", "frames/3-after", "frames/4-trace"]) mkdirSync(`${OUT}/${d}`, { recursive: true });

const browser = await chromium.launch({ channel: "msedge" });
const context = await browser.newContext({ viewport: size, deviceScaleFactor: 1, colorScheme: "dark", locale: "en-GB", timezoneId: "UTC" });
const page = await context.newPage();

/** A caption at the bottom of the page, through the DOM. */
async function caption(text) {
  await page.evaluate((t) => {
    let bar = document.getElementById("demo-caption");
    if (!bar) {
      bar = document.createElement("div");
      bar.id = "demo-caption";
      Object.assign(bar.style, {
        position: "fixed", left: "50%", bottom: "28px", transform: "translateX(-50%)", zIndex: "2147483647",
        maxWidth: "1100px", padding: "12px 24px", borderRadius: "12px", background: "rgba(10, 14, 22, 0.92)",
        border: "1px solid rgba(110, 160, 255, 0.55)", color: "#f1f5f9", textAlign: "center",
        font: '500 21px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif', boxShadow: "0 8px 30px rgba(0,0,0,.45)",
      });
      document.body.appendChild(bar);
    }
    bar.textContent = t;
  }, text);
}

let n = 0;
const shot = (dir) => page.screenshot({ path: `${OUT}/frames/${dir}/${String(n++).padStart(4, "0")}.png` });

const FROM_PART = Number(process.env.FROM_PART ?? 1);
let window = process.env.WINDOW ? `from=${process.env.WINDOW.replace(",", "&to=")}` : "from=now-12m&to=now";
if (FROM_PART <= 2) {
// --- 1. The live analytics ---------------------------------------------------------------------------
console.log("load: the checker's workload and the example shop, for ten minutes");
remote(`docker rm -f ledgerline-demo-load ledgerline-shop >/dev/null 2>&1; ${C} run -d --name ledgerline-demo-load -e DURATION_S=660 -e RATE=6 -e DEADLINE_S=60 checker workload /tmp/demo-load.json >/dev/null; docker run -d --name ledgerline-shop --network ledgerline_internal --network-alias shop -e SECONDS=600 -e RATE=2 -e FLAKY_SECONDS=0 -e DEADLINE_S=60 ledgerline-shop:latest >/dev/null`);
await pause(60_000); // warm up, so the charts have something on them

await page.goto(`${BASE}/`, { waitUntil: "load" });
await pause(4_000);
await caption("Ledgerline: card payments, payouts and a double-entry ledger. Analytics from ClickHouse, fed by Kafka.");
for (let i = 0; i < 4; i++) { await shot("1-live"); await pause(1_000); }

// --- 2. The ledger stopped, under load -------------------------------------------------------------------
await page.goto(`${BASE}/grafana/d/payments?kiosk&from=now-12m&to=now&refresh=5s&timezone=utc`, { waitUntil: "load" });
await pause(8_000);
const t0 = Date.now();
const phase = (s) =>
  s < 30 ? "Grafana, the payments dashboard: steady load, nothing waiting."
  : s < 180 ? "The ledger is stopped. Payments still get an answer (202), and wait for the worker: the backlog grows."
  : s < 240 ? "The ledger is back. The worker finishes every waiting step, with the same idempotency keys."
  : "Everything waiting is done: nothing lost, nothing done twice.";
let stopped = false, started = false;
while (Date.now() - t0 < 330_000) {
  const s = (Date.now() - t0) / 1000;
  if (!stopped && s >= 30) { remote(`${C} stop ledger >/dev/null 2>&1`); stopped = true; console.log("ledger stopped"); }
  if (!started && s >= 180) { remote(`${C} start ledger >/dev/null 2>&1`); started = true; console.log("ledger started"); }
  await caption(phase(s));
  await shot("2-outage");
  await pause(Math.max(0, 3_000 - ((Date.now() - t0) % 3_000)));
}

}

// --- 3. What paged, and the books ------------------------------------------------------------------------
await page.goto(`${BASE}/grafana/d/overview?kiosk&${window}&timezone=utc`, { waitUntil: "load" });
await pause(10_000);
await caption("The alerts that fired (red lines): ServiceDown, then PaymentsNotFinishing; both cleared on their own.");
for (let i = 0; i < 5; i++) { await shot("3-after"); await pause(1_000); }

// --- 4. A payment's trace through the outage --------------------------------------------------------------
const tree = remote(`cd ~/ledgerline && docker run --rm --network ledgerline_internal -v "$PWD/scripts:/s:ro" python:3.13.15-slim python3 /s/trace.py --search '{ name = "payment step" } && { status = error }' 1`);
writeFileSync(`${OUT}/trace.txt`, tree);
const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;");
await page.setContent(`<!doctype html><html><body style="margin:0;background:#0b0f17;color:#dbe4f0;font:15px/1.55 ui-monospace,Consolas,monospace">
  <div style="padding:36px 44px"><div style="font:600 22px system-ui;color:#fff;margin-bottom:6px">A payment's trace, from Tempo</div>
  <div style="font:15px system-ui;color:#94a3b8;margin-bottom:22px">Authorised while the ledger was down: the worker retried the step until it was back, in the same trace.</div>
  <pre style="margin:0;white-space:pre-wrap">${esc(tree).replace(/^(\s*)! (.*)$/gm, '$1<span style="color:#f87171">! $2</span>')}</pre></div></body></html>`);
await caption("Each row keeps the trace of the change that wrote it: the worker, the outbox and the webhooks continue it.");
for (let i = 0; i < 5; i++) { await shot("4-trace"); await pause(500); }

await browser.close();
remote("docker rm -f ledgerline-demo-load ledgerline-shop >/dev/null 2>&1 || true");
console.log(`frames in ${OUT}/frames`);
