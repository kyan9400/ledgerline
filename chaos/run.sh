#!/bin/bash
# A chaos run: a client keeps paying (checker workload) while this script breaks things on a schedule;
# then checker verify proves nothing was lost or done twice. Results go to eval/chaos/<scenario>.*.
#
#   chaos/run.sh <scenario> [seconds=120]      RATE=15 (flows a second), SEED=...
#
# Scenarios:
#   baseline        nothing breaks
#   kill-ledger     the ledger is killed (SIGKILL) three times, 10 s down each
#   kill-payments   the payments service is killed three times, mid-step
#   kill-postgres   PostgreSQL is killed twice: both services lose their database
#   kill-fraud      the fraud scorer is killed twice: payments go on, unscored, until it has rebuilt
#   kill-clickhouse ClickHouse is killed twice mid-ingestion: it must catch up, missing nothing, counting nothing twice
#   kill-kafka      Kafka is down for 50 s
#   partition       the ledger, then the other bank, cut off the network for 20 s
#   toxic           payments' connections to the ledger and the bank slowed, cut mid-answer, reset, hung
#   kafka-loss      Kafka loses all its data; the services refill it from their event logs
#   mixed           a bit of everything, in a seeded random order
set -euo pipefail
cd "$(dirname "$0")/.."
scenario=${1:?scenario}
duration=${2:-120}
rate=${RATE:-15}
seed=${SEED:-$(date +%s)}
C="docker compose -f deploy/compose.yaml -f deploy/chaos.yaml --env-file deploy/.env"
out=eval/chaos
mkdir -p "$out"
chmod 777 "$out"
rm -f "$out/$scenario".*
t0=0

event() { echo "$(( $(date +%s) - t0 ))s $*" | tee -a "$out/$scenario.events"; }
at() { local wait=$(( $1 - ($(date +%s) - t0) )); [ "$wait" -gt 0 ] && sleep "$wait"; return 0; }
container() { echo "ledgerline-$1-1"; }
kill9() { event "kill -9 $1"; docker kill -s KILL "$(container "$1")" >/dev/null; }
revive() { event "start $1"; docker start "$(container "$1")" >/dev/null; }
cut() { event "network cut: $1"; docker network disconnect ledgerline_internal "$(container "$1")"; }
mend() { event "network back: $1"; docker network connect --alias "$1" ledgerline_internal "$(container "$1")"; }
tox() { docker run --rm --network ledgerline_internal alpine:3.22 wget -qO- --header "Content-Type: application/json" --post-data "$2" "http://toxiproxy:8474$1" >/dev/null; }
toxic() { event "toxic on $1: $2 $3"; tox "/proxies/$1/toxics" "{\"name\": \"$2\", \"type\": \"$2\", \"stream\": \"${4:-downstream}\", \"attributes\": $3}"; }
calm() { event "toxics removed"; tox /reset '{}'; }

$C up -d --wait >/dev/null 2>&1
tox /reset '{}'

# A JVM on one CPU spends its first minute compiling: warm the services up first, and discard that.
$C run --rm -v "$PWD/$out:/out" -e DURATION_S=30 -e RATE="$rate" -e DEADLINE_S=60 \
  checker workload /out/warmup.log.json >/dev/null 2>&1 || true
rm -f "$out/warmup.log.json"

# The client, in the background.
docker rm -f ledgerline-workload >/dev/null 2>&1 || true
$C run -d --name ledgerline-workload -v "$PWD/$out:/out" -e DURATION_S="$duration" -e RATE="$rate" -e SEED="$seed" \
  -e READY_FILE="/out/$scenario.ready" checker workload "/out/$scenario.log.json" >/dev/null
# Setup (accounts) takes a few seconds; the schedule starts when the load does.
until [ -f "$out/$scenario.ready" ]; do
  if [ "$(docker inspect -f '{{.State.Running}}' ledgerline-workload)" != true ]; then
    docker logs ledgerline-workload 2>&1 | tail -5 >&2
    echo "the workload stopped before it was ready" >&2
    exit 1
  fi
  sleep 1
done
t0=$(date +%s)
event "load starts: $scenario, ${duration}s, $rate flows/s, seed $seed"

case "$scenario" in
  baseline) ;;
  kill-ledger | kill-payments)
    s=${scenario#kill-}
    for t in 20 55 90; do at $t; kill9 "$s"; at $((t + 10)); revive "$s"; done ;;
  kill-postgres)
    for t in 30 80; do at $t; kill9 postgres; at $((t + 10)); revive postgres; done ;;
  kill-fraud)
    for t in 25 75; do at $t; kill9 fraud; at $((t + 15)); revive fraud; done ;;
  kill-clickhouse)
    for t in 25 75; do at $t; kill9 clickhouse; at $((t + 15)); revive clickhouse; done ;;
  kill-kafka)
    at 20; kill9 kafka; at 70; revive kafka ;;
  partition)
    at 20; cut ledger; at 40; mend ledger
    at 70; cut otherbank; at 90; mend otherbank ;;
  toxic)
    at 10; toxic ledger latency '{"latency": 300, "jitter": 200}'
    at 35; calm
    at 40; toxic ledger limit_data '{"bytes": 150}'          # answers cut off after 150 bytes
    at 60; calm
    at 65; toxic ledger reset_peer '{"timeout": 50}'          # connections reset
    at 80; calm
    at 85; toxic ledger timeout '{"timeout": 3000}'           # hang, then close
    at 100; calm
    at 100; toxic otherbank timeout '{"timeout": 2500}'
    at 115; calm ;;
  kafka-loss)
    at 40
    event "Kafka loses its data: services stopped, Kafka's volume deleted"
    $C stop ledger payments >/dev/null 2>&1
    $C rm -sf kafka >/dev/null 2>&1
    docker volume rm ledgerline_kafka_data >/dev/null
    $C up -d --wait kafka >/dev/null 2>&1
    event "Kafka back, empty: refilling the outboxes from the event logs"
    $C run --rm checker republish ledger | tee "$out/$scenario.republish-ledger.json" >/dev/null
    $C run --rm checker republish payments | tee "$out/$scenario.republish-payments.json" >/dev/null
    $C up -d ledger payments >/dev/null 2>&1
    event "services started" ;;
  mixed)
    faults=(ledger payments postgres kafka partition-ledger partition-bank toxic-cut toxic-reset)
    RANDOM=$seed
    for t in 15 40 65 90; do
      at $t
      f=${faults[$((RANDOM % ${#faults[@]}))]}
      case $f in
        partition-ledger) cut ledger; sleep 15; mend ledger ;;
        partition-bank) cut otherbank; sleep 15; mend otherbank ;;
        toxic-cut) toxic ledger limit_data '{"bytes": 150}'; sleep 15; calm ;;
        toxic-reset) toxic ledger reset_peer '{"timeout": 50}'; sleep 15; calm ;;
        kafka) kill9 kafka; sleep 15; revive kafka ;;
        *) kill9 "$f"; sleep 8; revive "$f" ;;
      esac
    done ;;
  *) echo "unknown scenario $scenario" >&2; exit 2 ;;
esac

docker wait ledgerline-workload >/dev/null
event "load done"
docker logs ledgerline-workload 2>/dev/null | sed -n '/^{/,$p' > "$out/$scenario.workload.json"
docker rm ledgerline-workload >/dev/null
# Whatever broke is back by now; make sure.
tox /reset '{}'
$C up -d --wait >/dev/null 2>&1
event "verifying"
set +e
$C run --rm -v "$PWD/$out:/out" checker verify "/out/$scenario.log.json" 2>/dev/null | sed -n '/^{/,$p' > "$out/$scenario.json"
event "verified: $(grep -m1 '"ok"' "$out/$scenario.json")"
rm -f "$out/$scenario.log.json" "$out/$scenario.ready"   # the log is large; the summaries stay
# Analytics, fed from Kafka, must agree with the databases too (it waits for ClickHouse to catch up).
$C run --rm checker analytics 2>/dev/null | sed -n '/^{/,$p' > "$out/$scenario.analytics.json"
event "analytics: $(grep -m1 '"ok"' "$out/$scenario.analytics.json")"
grep -q '"ok" : true' "$out/$scenario.json" && grep -q '"ok" : true' "$out/$scenario.analytics.json"
