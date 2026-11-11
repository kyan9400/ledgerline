#!/bin/bash
# A merchant, end to end, while things break: the example shop (sdk/examples/shop.ts) takes card payments
# through the SDK for a minute. Its own webhook endpoint refuses everything for the first 30 s, and the
# payments service is killed at 25 s and comes back at 35 s, mid-payment. The shop then proves it heard
# about every change, by webhook (signed) and by the event stream; then the reconciliation and analytics
# must hold too. Results: eval/merchant/.
#
#   chaos/merchant.sh
set -euo pipefail
cd "$(dirname "$0")/.."
C="docker compose -f deploy/compose.yaml -f deploy/chaos.yaml --env-file deploy/.env"
out=eval/merchant
mkdir -p "$out"
t0=$(date +%s)
event() { echo "$(( $(date +%s) - t0 ))s $*" | tee -a "$out/e2e.events"; }
rm -f "$out"/e2e.*

$C up -d --wait >/dev/null 2>&1
docker rm -f ledgerline-shop >/dev/null 2>&1 || true
docker run -d --name ledgerline-shop --network ledgerline_internal --network-alias shop \
  -e SECONDS=60 -e RATE=4 -e FLAKY_SECONDS=30 ledgerline-shop:latest >/dev/null
t0=$(date +%s)
event "the shop starts paying; its webhook endpoint refuses everything for 30 s"
sleep 25; event "kill -9 payments"; docker kill -s KILL ledgerline-payments-1 >/dev/null
sleep 10; event "start payments"; docker start ledgerline-payments-1 >/dev/null
docker wait ledgerline-shop >/dev/null
docker logs ledgerline-shop 2>/dev/null | sed -n '/^{/,$p' > "$out/e2e.json"
docker rm ledgerline-shop >/dev/null
event "the shop is done: $(grep -m1 '"ok"' "$out/e2e.json")"

# Once nothing is in progress (it answers 409 until then).
for _ in $(seq 1 60); do
  reconcile=$(docker run --rm --network ledgerline_internal alpine:3.22 wget -qO- http://payments:8080/internal/reconcile 2>/dev/null || true)
  echo "$reconcile" | grep -q '"quiet":true' && break
  sleep 2
done
echo "$reconcile" > "$out/e2e.reconcile.json"
event "reconciliation: $(echo "$reconcile" | grep -o '"ok":[a-z]*' | head -1)"
$C run --rm checker analytics 2>/dev/null | sed -n '/^{/,$p' > "$out/e2e.analytics.json"
event "analytics: $(grep -m1 '"ok"' "$out/e2e.analytics.json")"
grep -q '"ok": true' "$out/e2e.json" && grep -q '"ok":true' "$out/e2e.reconcile.json" && grep -q '"ok" : true' "$out/e2e.analytics.json"
