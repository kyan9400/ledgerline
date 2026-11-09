# Ledgerline

**A payments platform built like a bank's: money is never created, lost or moved twice.** At its core
is a double-entry ledger over an append-only event log, in Kotlin and Spring Boot on PostgreSQL. Each
property it promises is checked by tests that try hard to break it.

> Work in progress: stage 1 of 8, the ledger core, is done (see [the plan](#roadmap)). All accounts and
> money in it are made up.

## The ledger

- **Double entry:** every transaction is a set of entries that sums to zero per currency. A transfer is
  one debit and one credit; a posting may have up to 100 entries. Amounts are whole minor units
  (kopecks, cents) in 64-bit integers, never floating point, with overflow refused.
- **Event-sourced:** the source of truth is an append-only log of events (`AccountOpened`,
  `TransactionRecorded`, `Posted`). A database trigger refuses to update, delete or truncate it.
  Balances, entries and transactions are projections, written in the same database transaction and
  rebuildable from the events.
- **No overdraft, under any concurrency:** a posting locks its accounts' balances in id order, so two
  postings over the same accounts wait for each other instead of deadlocking. Funds are checked before
  anything is written. Two more lines of defence:
  - each account's events are numbered, and a unique `(stream, version)` refuses a second writer;
  - a CHECK constraint refuses a negative balance on an account that may not have one.
- **Idempotent writes:** every write needs an `Idempotency-Key`. The key is claimed in the same
  transaction as the write.
  - A retry gets the first answer byte for byte, a refusal included, marked `Idempotent-Replayed: true`.
  - The same key with a different request is refused.
  - Concurrent requests with one key wait for the first and get its answer.
- **Reversals, not edits:** a mistake is undone by a new transaction with every entry negated, at most
  once. It is refused if the money has already been spent.
- **An audit that recomputes the books** (`GET /internal/audit`). On one consistent snapshot, it checks
  that:
  - every currency sums to zero across all accounts;
  - every transaction balances;
  - every balance, entry and transaction matches what replaying the events gives;
  - nothing is overdrawn.
- **Load shedding:** at most 32 requests in flight. The next waits up to 200 ms, then gets a fast `503`
  with `Retry-After`, which is safe to retry with the same key.

```
POST /v1/accounts                     open an account (customer, merchant, or system: may go negative)
GET  /v1/accounts/{id}                balance and version;   GET /v1/accounts?code=...
GET  /v1/accounts/{id}/entries        statement, newest first, paged
POST /v1/transfers                    from, to, amount
POST /v1/transactions                 any balanced set of entries
POST /v1/transactions/{id}/reversal   undo, once
GET  /v1/transactions/{id}
```

Refusals are [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem details with a stable `code`:
`insufficient_funds`, `already_reversed`, `idempotency_key_reused`, `busy` and so on.

## Tested to break

The tests run against a real PostgreSQL:

- **A model, and random operations:** Kotest generates 150 random sequences of up to 40 operations:
  transfers, multi-entry postings, reversals of earlier transactions, and retries of earlier requests
  with their keys. Each runs against the ledger and against an in-memory model a few lines long.
  - After every operation, the outcome (done, or refused and why) and every balance must agree with the
    model.
  - After every sequence, the audit must pass.
  - A retry must never run the write again.
- **Concurrency:**
  - 400 transfers race, 32 at a time, for money for 100: exactly 100 go through, and the balance ends
    at 0;
  - 3,000 random transfers run in every direction between ten accounts: the total is unchanged and
    none is overdrawn;
  - 30 requests with one key at once give one transaction and one identical answer.
- **The tests catch what they claim to:** with the row lock removed on purpose, both race tests fail.
  The event log's version constraint also refuses the conflicting writes, so even then the books
  aren't corrupted.
- **The API:** transfers, refusals, idempotent replays, reversals, statements and paging; an append-only
  event log; a passing audit.

## Measured

A k6 load test ([`load/ledger.js`](load/ledger.js), results in
[`eval/load-ledger.json`](eval/load-ledger.json)) runs transfers between 1,000 accounts at rising rates,
then all into 5 merchant accounts (every transfer locks a merchant's row: the worst case for
contention). The ledger and PostgreSQL each run on 1 CPU, the ledger in 512 MB.

| Transfers/s asked | Done/s | Shed (fast 503) | p50 | p95 | p99 | Failed |
| --- | --- | --- | --- | --- | --- | --- |
| 100 | 100 | 0% | 4 ms | 10 ms | 37 ms | 0% |
| 250 | 250 | 0% | 3 ms | 11 ms | 42 ms | 0% |
| 500 | 440 | 12% | 237 ms | 332 ms | 392 ms | 0% |
| 1,000 | 429 | 57% | 205 ms | 326 ms | 401 ms | 0% |
| 250, all into 5 merchants | 250 | 0.02% | 3 ms | 10 ms | 71 ms | 0% |

The ledger sustains about 430–440 transfers a second, with both CPUs at their caps. Beyond that it
refuses the excess quickly and keeps serving what it can. Afterwards the audit passed over all 152,946
transactions and 460,850 events.

The first run of this test found the reason for the shedding. With virtual threads every request gets a
thread at once, so an overload doesn't queue in the web server: it piles up inside the service. At
1,000 a second, 1,500 requests were waiting for 16 database connections when the heap ran out and the
JVM died. The books were intact even then (the audit passed afterwards), but a ledger mustn't fall over
under load.

## Running it

```bash
cp deploy/.env.example deploy/.env      # set POSTGRES_PASSWORD (openssl rand -hex 24)
docker compose -f deploy/compose.yaml --env-file deploy/.env up -d --build
```

The tests, with a throwaway PostgreSQL in Docker:

```bash
ledger/scripts/test-in-docker.sh
```

Or with your own PostgreSQL: `LEDGER_TEST_DB_URL=jdbc:postgresql://... ledger/gradlew -p ledger build`.

The stack is Kotlin 2.3, Spring Boot 4.1 (Spring MVC on virtual threads, JdbcClient, Flyway), Java 25
and PostgreSQL 18.

## Roadmap

| Stage | |
| --- | --- |
| 1. Ledger core | done |
| 2. Payments | Card payments (authorise, capture, refund) and transfers to another bank as sagas; an outbox into Kafka |
| 3. Chaos | Services killed and networks cut mid-payment, with a checker proving nothing is lost or duplicated |
| 4. Fraud | A simulator with known fraud, real-time features from Kafka, a model scored against the truth |
| 5. Analytics | ClickHouse fed from Kafka; a Vue 3 dashboard |
| 6. Merchants | A gRPC API, signed webhooks, an SDK |
| 7. Production | OpenTelemetry tracing, Grafana, alerts |
| 8. Publishing | Demo, write-up |
