#!/bin/bash
# Do the alerts work? Under steady load (checker workload, and the example shop over gRPC), each scenario
# breaks one thing for long enough to matter, and records which alerts fire, how soon after the fault,
# and how soon after it they clear. It starts with a run where nothing breaks: no alert may fire then.
# Results: eval/alerts/ (alerts.py turns the timeline into the table).
#
#   chaos/alerts.sh [scenario ...]      default: all, in this order
#
#   baseline        nothing breaks, 8 minutes                    expect: nothing
#   ledger-down     the ledger is stopped for 3 minutes          expect: ServiceDown, PaymentsNotFinishing
#   kafka-down      Kafka is stopped for 3 minutes               expect: OutboxLagging
#   fraud-down      the fraud scorer is stopped for 4 minutes    expect: ServiceDown, FraudScorerFailingOpen
#   slow-ledger     the ledger answers payments 1.5 s late, 15 m expect: DependencySlow
#   postgres-down   PostgreSQL is stopped for 2 minutes          expect: DatabaseUnreachable
#   webhooks-down   the shop's webhook endpoint refuses, 7 m     expect: WebhooksFailing
#   books-wrong     an account's balance is changed behind the ledger's back (then put back)
#                                                                expect: LedgerBooksWrong
#
# The burn-rate alerts page on how much of the error budget goes, not on any fault, so a smaller fault
# is left to the cause alerts (a slow ledger: DependencySlow). chaos/alerts.py also lists what follows
# from a fault (LoadShedding when requests wait on a slow dependency, say): allowed, and reported.
#
# Prometheus and Alertmanager read their files at start: restart them after changing alerts.yaml.
set -euo pipefail
cd "$(dirname "$0")/.."
C="docker compose -f deploy/compose.yaml -f deploy/chaos.yaml --env-file deploy/.env"
out=eval/alerts
mkdir -p "$out"
scenarios=("$@")
[ ${#scenarios[@]} -eq 0 ] && scenarios=(baseline ledger-down kafka-down fraud-down slow-ledger postgres-down webhooks-down books-wrong)

event() { echo "$(date +%s) $*" | tee -a "$out/events.log"; }
tox() { docker run --rm --network ledgerline_internal alpine:3.22 wget -qO- --header "Content-Type: application/json" --post-data "$2" "http://toxiproxy:8474$1" >/dev/null; }
query() { docker run --rm --network ledgerline_internal alpine:3.22 wget -qO- "http://prometheus:9090/api/v1/query?query=$1"; }
firing() { query 'ALERTS%7Balertstate%3D%22firing%22%7D' | grep -o '"alertname":"[A-Za-z]*"' | sort -u | tr '\n' ' '; }
shop() {  # seconds, flaky seconds
  docker rm -f ledgerline-shop >/dev/null 2>&1 || true
  docker run -d --name ledgerline-shop --network ledgerline_internal --network-alias shop \
    -e SECONDS="$1" -e RATE=3 -e FLAKY_SECONDS="$2" -e DEADLINE_S=60 ledgerline-shop:latest >/dev/null
}
active() { query 'ALERTS' | grep -o '"alertname":"[A-Za-z]*"' | sort -u | tr '\n' ' '; }  # firing or pending
# Until no alert fires or is pending (at most 15 minutes): the next scenario starts from quiet.
quiet() {
  for _ in $(seq 1 90); do
    [ -z "$(active)" ] && return 0
    sleep 10
  done
  event "still active after 15 minutes: $(active)"
}

rm -f "$out"/events.log "$out"/timeline.log
# The audit every minute (10 by default), so books-wrong is found in this run's time.
LEDGER_AUDIT_EVERY=1m $C up -d --wait >/dev/null 2>&1
tox /reset '{}'

# The timeline: every 10 s, what is pending and firing.
docker rm -f ledgerline-alertwatch >/dev/null 2>&1 || true
docker run -d --name ledgerline-alertwatch --network ledgerline_internal alpine:3.22 sh -c \
  'while true; do echo "$(date +%s) $(wget -qO- "http://prometheus:9090/api/v1/query?query=ALERTS")"; sleep 10; done' >/dev/null

# Steady load for all of it (the workload stops when told; its log isn't needed).
docker rm -f ledgerline-workload >/dev/null 2>&1 || true
$C run -d --name ledgerline-workload -e DURATION_S=100000 -e RATE=8 -e DEADLINE_S=60 checker workload /tmp/alerts.log.json >/dev/null
sleep 90   # warm-up: accounts, and the JVMs' first compilations
quiet

for s in "${scenarios[@]}"; do
  # The host's disk is small, and shared: stop rather than fill it.
  free=$(df -m --output=avail / | tail -1)
  if [ "$free" -lt 800 ]; then
    event "stopped: only $free MB of disk left"
    break
  fi
  case $s in
    baseline)
      shop 500 0; event "start $s"; sleep 480; event "end $s" ;;
    ledger-down)
      shop 330 0; sleep 60; event "start $s"; $C stop ledger >/dev/null 2>&1
      sleep 180; LEDGER_AUDIT_EVERY=1m $C start ledger >/dev/null 2>&1; event "end $s" ;;
    kafka-down)
      shop 330 0; sleep 60; event "start $s"; $C stop kafka >/dev/null 2>&1
      sleep 180; $C start kafka >/dev/null 2>&1; event "end $s" ;;
    fraud-down)
      shop 390 0; sleep 60; event "start $s"; $C stop fraud >/dev/null 2>&1
      sleep 240; $C start fraud >/dev/null 2>&1; event "end $s" ;;
    slow-ledger)
      shop 1020 0; sleep 60; event "start $s"
      tox /proxies/ledger/toxics '{"name": "latency", "type": "latency", "stream": "downstream", "attributes": {"latency": 1500, "jitter": 0}}'
      sleep 900; tox /reset '{}'; event "end $s" ;;
    postgres-down)
      shop 270 0; sleep 60; event "start $s"; $C stop postgres >/dev/null 2>&1
      sleep 120; $C start postgres >/dev/null 2>&1; event "end $s" ;;
    webhooks-down)
      # The endpoint comes back after 7 minutes, and the shop stays to take the retries.
      shop 900 420; event "start $s"; sleep 420; event "end $s" ;;
    books-wrong)
      # An account of its own, used by nothing else: the change can't leak into anyone's postings.
      id=$(docker run --rm --network ledgerline_internal alpine:3.22 wget -qO- --header "Content-Type: application/json" \
        --header "Idempotency-Key: alerts-$(date +%s)" --post-data "{\"code\": \"alerts:tampered:$(date +%s)\", \"currency\": \"RUB\", \"kind\": \"customer\"}" \
        http://ledger:8080/v1/accounts | grep -o '"id":"[0-9a-f-]*"' | cut -d'"' -f4)
      psql() { $C exec -T postgres psql -q -U ledger -d ledger -c "$1" >/dev/null; }
      event "start $s"; psql "UPDATE balances SET balance = balance + 100 WHERE account_id = '$id'"
      for _ in $(seq 1 30); do firing | grep -q LedgerBooksWrong && break; sleep 10; done
      psql "UPDATE balances SET balance = balance - 100 WHERE account_id = '$id'"; event "end $s" ;;
    *) echo "unknown scenario $s" >&2; exit 2 ;;
  esac
  quiet
  event "quiet after $s"
  docker rm -f ledgerline-shop >/dev/null 2>&1 || true
done

docker rm -f ledgerline-workload >/dev/null 2>&1 || true
docker logs ledgerline-alertwatch > "$out/timeline.log" 2>/dev/null
docker rm -f ledgerline-alertwatch >/dev/null
$C up -d >/dev/null 2>&1   # the audit back to its usual schedule
python3 chaos/alerts.py "$out"
