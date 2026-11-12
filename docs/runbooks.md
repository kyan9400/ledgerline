# Runbooks

One section per alert in `deploy/observability/alerts.yaml`: what it means, what to look at first, and
what to do. Grafana is at `http://localhost:18300` through `ssh -L 18300:127.0.0.1:18300 <host>`; the
"overview" and "payments" dashboards are linked from each other. To find out what happened to one
request, find its trace: Grafana → Explore → Tempo, or `scripts/trace.py --search '<TraceQL>'`.

**Severity.** `page`: money isn't moving, or the books are wrong; act now. `ticket`: something is degraded
and will hurt if left; act today. Alertmanager holds back the pages that follow from a `ServiceDown`.

## ServiceDown

Prometheus hasn't been able to scrape a service for a minute: it is down, restarting, or out of reach.

- `docker compose ps`: is it restarting? `docker compose logs --tail 100 <service>`.
- The ledger or payments down: payments wait (their steps resume when it is back; nothing is lost or done
  twice, as the chaos runs show). The fraud scorer down: card payments go through unscored. ClickHouse
  down: the analytics dashboard is stale; it catches up from Kafka.
- An OOM (`ExitOnOutOfMemoryError` in the logs): look at the heap panel; raise `mem_limit` or find the leak.

## DatabaseUnreachable

A service's query to its database has failed for 30 seconds (`ledgerline.database.up`, probed every 5 s).
Its requests fail with 500s meanwhile; nothing is lost (a write either committed or didn't, and the
clients retry with the same idempotency key).

- Is PostgreSQL up (`docker compose ps postgres`, its logs)? Out of connections (`max_connections`)? Out
  of disk?
- Only one service affected: its credentials, or its pool ("Database connections in use").

## ApiErrorBudgetBurn

Over 7.2% of API requests fail on the server's side, over the last 5 minutes and the last hour: at this
rate a month's error budget (0.5%) is gone in two days.

- Overview → "Server errors per second": which service, which status. 503s with `code: busy` are load
  shedding (see LoadShedding); 500s are faults.
- Most often the database: is PostgreSQL up, and are the pools exhausted ("Database connections in use",
  "waiting")? Then the logs, and a failing request's trace (Tempo: `{ status = error }`).

## ApiErrorBudgetBurnSlow

The same, slower: over 3% of requests have failed for 30 minutes and 6 hours. Not an outage; a fault that
has become normal (one endpoint that always fails, a client sending what the server can't take).
Overview → "Server errors per second" by status, and the traces of those requests.

## ApiLatencyBudgetBurn

Over 14.4% of API requests take more than a second (the SLO: 99% within 1 s).

- Overview → "Latency p99": click a dot (an exemplar) for the trace of a slow request, and see which span
  takes the time.
- "Dependencies: p99 as payments sees them": a slow ledger (or a slow network to it) makes payments slow;
  past its timeout (2 s) the step is left to the worker and the client gets 202.
- The JVM: GC pauses, heap; the database: connections waiting.

## PaymentsNotFinishing

A payment or refund has waited more than a minute for its next step. The clients got their answers (a
202), but the money isn't moving: usually the ledger is unreachable.

- Payments → "Oldest waiting for its next step", "Retries and stuck steps"; Overview → "Dependencies:
  calls without an answer".
- A waiting row's trace shows each attempt: Tempo `{ name = "payment step" && status = error }`.
- When the ledger answers again, the worker drains the backlog by itself; nothing needs doing to the rows.
  Then `GET /internal/reconcile` on payments should report `ok: true`.

## PayoutsNotFinishing

A payout has waited over 10 minutes: the other bank isn't answering, or isn't settling. The money stays
reserved in clearing (it is never given back on a guess). Check the bank's side; the worker keeps asking.

## DependencySlow

Half of payments' calls to the ledger (or the fraud scorer, or the bank) have taken over 500 ms for 10
minutes. Requests wait on it, so they are slow, and fewer arrive (clients wait; the service sheds
load): the hour's share of slow requests can stay under the latency SLO's page for a long time.

- Overview → "Dependencies: p99 as payments sees them"; is the dependency itself slow (its own latency,
  its database), or the network to it?

## PaymentsStuck

The ledger refused a step that should never be refused (releasing a hold, capturing it). The row is
parked (`next_attempt_at` NULL) with the refusal in `last_error`, for a person:

```sql
SELECT id, state, last_error, updated_at FROM payments WHERE state IN ('authorising', 'capturing', 'voiding') AND next_attempt_at IS NULL;
```

Find out why the ledger refused (its logs; the ledger's audit), fix the cause, then set `next_attempt_at
= now()` to let the worker try again. Never change balances by hand.

## OutboxLagging

A service's events reach Kafka over 30 seconds late: Kafka is down or unreachable. The services keep
working (the outbox grows in PostgreSQL, and drains when Kafka is back); what is behind is everything fed
from Kafka: the fraud features, analytics, the merchants' event streams.

- Kafka's container and logs. If Kafka lost its data: `chaos/run.sh kafka-loss` shows the recovery
  (`checker republish`).

## FraudScorerFailingOpen

Over 10% of card payments go through without a fraud score: the scorer is down, still rebuilding its
features from Kafka after a start (it answers 503 until it has), or slower than the 150 ms payments waits.
Payments are not held up (that is the point of failing open), but fraud gets through meanwhile.

- Is it up and ready (`/health`)? Payments → "Fraud scorer: time to score".

## WebhooksFailing

Over half of webhook deliveries are refused or time out. Usually one merchant's endpoint is down
(their problem, but they will ask): the deliveries are retried with backoff for about 27 minutes (12 tries), then kept
as failed, and `POST /internal/merchants/{id}/webhooks/redeliver` sends them again.

```sql
SELECT merchant_id, count(*), max(last_error) FROM webhook_deliveries WHERE state = 'pending' AND attempts > 0 GROUP BY 1;
```

## LoadShedding

A service turns away over one request a second with 503 `busy`: more requests at once than it lets in
(`admission.max-in-flight`). Clients retry safely (with the same Idempotency-Key). Sustained: the
service needs more capacity, or something downstream (the database) has slowed it.

## LedgerBooksWrong

The ledger's audit (every 10 minutes) found the books inconsistent: a currency that doesn't sum to zero,
a balance that doesn't match its events, an account below zero that may not be. This should never happen.

- `GET /internal/audit` on the ledger: which check, how many.
- The event log is the source of truth (and append-only); the projections (balances, entries) are what
  can be wrong. Find what wrote to them other than the ledger.
- Stop payouts until it is understood.

## LedgerAuditNotRunning

No audit has completed for 30 minutes: it fails (the ledger's log says why) or takes too long ("Audit:
took"). Without it LedgerBooksWrong can't fire.
