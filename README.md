# Ledgerline

**A payments platform built like a bank's: money is never created, lost or moved twice.** At its core
is a double-entry ledger over an append-only event log. Card payments and payouts to other banks run on
it as state machines and sagas, and every change reaches Kafka through a transactional outbox. It is
written in Kotlin and Spring Boot on PostgreSQL and Kafka. Each property it promises is checked by tests
that try hard to break it.

> Work in progress: stages 1 to 6 of 8 are done (see [the plan](#roadmap)). All accounts and money in
> it are made up.

```
               ┌────────────────┐  HTTP + idempotency keys  ┌────────────────┐
  clients ────▶│    payments    │──────────────────────────▶│     ledger     │◀──── clients
               │ state machines │                           │  double entry  │
               │ sagas, worker  │──▶ otherbank (simulated)  │   event log    │
               └───────┬────────┘                           └───────┬────────┘
                       │ outbox                                     │ outbox
                       ▼                                            ▼
               ┌──────────────────────────── Kafka ─────────────────────────────┐
               │  payments.events (by payment)        ledger.events (by account) │
               └─────────────────────────────────────────────────────────────────┘
```

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
POST /v1/accounts                     open an account (customer, merchant, or system)
GET  /v1/accounts/{id}                balance and version;   GET /v1/accounts?code=...
GET  /v1/accounts/{id}/entries        statement, newest first, paged
POST /v1/transfers                    from, to, amount
POST /v1/transactions                 any balanced set of entries
POST /v1/transactions/{id}/reversal   undo, once
GET  /v1/transactions/{id}
```

Refusals are [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem details with a stable `code`:
`insufficient_funds`, `already_reversed`, `idempotency_key_reused`, `busy` and so on.

## Payments

The payments service keeps no money. It keeps what each payment, refund and payout is doing, as a state
machine in its own database, and moves the money with ledger postings. It has three accounts of its own
in the ledger per currency: **holds**, **clearing** and, per bank, **settlement**.

```
card payment   authorising ─▶ authorised ─▶ capturing ─▶ captured ─▶ refunds: refunding ─▶ refunded | failed
                     │              └─────▶ voiding ───▶ voided  (on request, or when the hold expires)
                     └─▶ declined
payout         reserving ─▶ submitting ─▶ submitted ─▶ settling ─▶ settled
                   │             │             └─────▶ returning ─▶ returned
                   └─▶ refused   └─────────────────────▶ returning
```

- **Authorise** moves the amount from the customer to holds; **capture** posts holds → merchant, and the
  uncaptured rest back to the customer, in one posting; **void** and **expiry** post holds → customer;
  a **refund** posts merchant → customer, never more in total than was captured. If the merchant hasn't
  the money, the refund fails and the amount stays refundable.
- **Each step is one ledger posting with a key fixed by the payment and the step**
  (`payments:payment:<id>:capture`). The API records the intent (the -ing state) and then takes the step
  at once. If it can't finish (the ledger is slow, the service dies), a worker takes it later. A retry,
  from anywhere, lands on the first try's result, because the ledger's idempotency replays it.
- **Two drivers can't both win:** every change of state is conditional on the version it was decided
  from, and the worker leases each row before taking a step, so several instances share the work.
- **Payouts are a saga** against another bank: reserve the money in clearing, send the payout, wait for
  the bank to settle it, then post clearing → settlement; or, if the bank refuses or returns it, post
  clearing → customer.
- **A timeout at the bank is not a refusal.** It says nothing about whether the bank got the payout, so
  the money stays reserved and the payout is sent again, by the same id (the bank's API is idempotent by
  it), until the bank answers. Money is never given back on a guess.
- **Reconciliation** (`GET /internal/reconcile`) checks the service's records against the ledger's
  balances and the bank's records:
  - holds holds exactly the authorised payments;
  - clearing holds exactly the payouts on their way;
  - settlement is exactly what has settled;
  - the bank has each payout it accepted once, with the same amount and outcome;
  - each payment's refunds add up.

```
POST /v1/payments                     authorise: customer, merchant, amount
POST /v1/payments/{id}/capture        all, or { amount } of it
POST /v1/payments/{id}/void
POST /v1/payments/{id}/refunds        { amount }
POST /v1/payouts                      account, amount, bank, beneficiary
GET  /v1/payments/{id}  /v1/payments/{id}/refunds  /v1/refunds/{id}  /v1/payouts/{id}
```

A write answers with the resource: `201` or `200` when its step is done, `202` while it's still in
progress (then `GET` it).

**The other bank** ([`otherbank`](otherbank/src/main/kotlin/dev/ledgerline/otherbank/OtherBank.kt)) is a
simulation that misbehaves on purpose. It fails a share of its answers. It answers a share only after the
caller has stopped waiting, having done the work. It accepts some payouts and returns them later. It
refuses closed accounts. It keeps a journal, so a restart forgets nothing.

## Events: the outbox into Kafka

Each service keeps an **append-only event log** of its own: the ledger, of every account and transaction;
the payments service, of every change of state of a payment, refund or payout. A database trigger refuses
to change or delete it. Each event also goes to an `outbox` table, **in the same database transaction as
the change**, so an event is published if and only if its change commits. A relay sends the outbox to
Kafka in order (an idempotent producer, `acks=all`) and deletes what Kafka has acknowledged.

- **Each stream stays in order.** Two changes to one account or payment lock the same row, so the
  second's outbox row is written only after the first commits. The relay reads in that order, and the
  stream is the Kafka key, so it stays in order in its partition.
- **At least once, exactly once in effect.** If the relay dies between sending and deleting, it sends
  the batch again. Every event has an id (`stream:version`), and a consumer skips a version it has
  already applied.
- **Kafka can be down; the services can't tell.** Writes go on, the outbox grows (`outbox.lag.seconds`),
  and it drains when Kafka is back.
- **The whole history:** the ledger's events from before the outbox existed went through it too, so a
  consumer can rebuild every account from the start of the topic.
- **Kafka isn't the system of record.** If Kafka loses its data, `checker republish` puts each
  service's whole event log back in its outbox, in order, and the relays send it again. Writes pause
  while it runs: it takes the event log and the relay's lock.
- **The checker** ([`checker`](checker/src/main/kotlin/dev/ledgerline/checker/Checker.kt)) does exactly
  that. It rebuilds every account's balance and version from `ledger.events`, and every payment's,
  refund's and payout's state from `payments.events`, from Kafka alone. Then it compares them with the
  services' databases.

## Fraud

Before a card payment's money moves, the payments service asks a fraud scorer
([`fraud`](fraud/fraud/service.py), Python). The scorer has a LightGBM model and real-time features for
every customer and merchant, which it keeps from `payments.events`.

- **Features from Kafka:**
  - the customer's attempts and amounts in the last minute, 10 minutes and hour;
  - the distinct merchants they used in those 10 minutes, and their tiny test payments;
  - this amount against their usual and their largest;
  - whether they have paid this merchant before, and how often;
  - the time since their last attempt;
  - the merchant's recent volume.

  On start, the scorer rebuilds them from the start of the topic: 72,000 events in 1.4 s. A request
  counts at once, by its payment id, so a burst doesn't wait for the topic. Its event, when it comes,
  is skipped as already seen.
- **One feature module, online and offline.** Training replays the topic through the same code the
  scorer runs ([`features.py`](fraud/fraud/features.py)). Replaying a live run gives the same score as
  live for 99.98% of payments, and the same decision for all of them
  ([`eval/fraud/parity.json`](eval/fraud/parity.json)).
- **Asked once, and it fails open.** The verdict is kept on the payment, so a retried step goes by the
  same one. If the scorer doesn't answer within 150 ms (it is down, or still rebuilding), the payment
  goes on, marked `unscored`. A fraud check that can stop every card payment is a bigger outage than the
  fraud it stops.
- **Three modes:**
  - `off`;
  - `shadow`: score and record, never decline, to measure a model before trusting it;
  - `enforce`: a decline verdict declines the payment as `fraud_suspected`, before any money is held.

### Measured against the truth

A simulator ([`sim.py`](fraud/fraud/sim.py)) drives the real API. It mixes customers with habits and
stolen cards, and writes down which attempts were fraud; nothing in the payments system sees that.

- **Customers** have favourite merchants, a usual amount and a pace: about one attempt a minute, since
  time here is fast. Now and then they try somewhere new. 3% of their attempts are a splurge: several
  times their usual, at a merchant they've never used.
- **Fraud** comes as episodes on a stolen card:
  - card testing: a string of payments under 5 roubles, seconds apart, then a large one;
  - account takeover: several large payments at new merchants;
  - a single large hit, which alone looks just like a splurge.

The model is trained on 25 minutes of shadow-mode traffic: 10,076 attempts, 6.6% of them fraud. The
split is by time: the first 60% to learn from, the next 15% to set the threshold (at most 1% of genuine
attempts declined), and the last 25% to test on. The rules a fraud analyst would write first are the
baseline, with their thresholds tuned on the same data to the same budget
([`eval/fraud/offline.json`](eval/fraud/offline.json)).

| On the test part (2,519 attempts) | Fraud caught | Fraud money caught | Genuine declined | Precision |
| --- | --- | --- | --- | --- |
| **Model** (LightGBM, 16 features; PR-AUC 0.96) | **88%** | **82%** | 0.7% | 91% |
| Rules, tuned to the same budget | 37% | 24% | 1.0% | 75% |
| Rules, as first written | 94% | 94% | 75% | 9% |

Then **live, in enforce mode**: 15 minutes of new customers and cards (another seed), the model
declining as they pay ([`eval/fraud/live.json`](eval/fraud/live.json)):

- **Fraud stopped:** 91.9% of the 494 fraud attempts were declined, and 86% of the fraud money. Of 65
  stolen cards, 41 were stopped at their first attempt, and 13.6% of the money at risk got through.
- **By pattern:** card testing was stopped every time, takeovers 70%, single large hits 4 of 6.
- **The cost:** 1.6% of genuine attempts were declined, against 0.7% expected offline. A third of
  genuine splurges were declined: they look like the single large hit.
  - It isn't skew between serving and training: replaying the live run offline gives the same
    decisions.
  - It is the threshold: it was set where 1% of genuine validation attempts would be declined, and that
    rested on 14 of them. On another population the rate came out higher. A bigger shadow window to set
    it on is the fix.
- **The time:** authorisation took 16 ms at the median and 27 ms at p95, against 15 and 25 in shadow
  mode. The scoring itself took 1–2 ms.
- **Killing the scorer** is a chaos scenario: payments go on unscored while it is down, then it rebuilds
  and scores again. Nothing else changes.

## Analytics

![The dashboard](docs/screenshots/dashboard.png)

ClickHouse reads both topics with its Kafka engine ([`init.sql`](deploy/clickhouse/init.sql)), and
materialized views turn the events into tables:

- every ledger event, and from them every posting and every account;
- every payment event, and from them each payment, refund and payout as it is now.

**Duplicates count once.** Delivery is at least once: the relays may send an event twice, and the Kafka
engine commits only after the views have written. So each table is a `ReplacingMergeTree` keyed by the
event's identity (`stream`, `version`), and is read with `FINAL`.

**It agrees with the ledger, to the kopeck.** `checker analytics` compares ClickHouse, fed only from
Kafka, with the services' databases:

- the same number of distinct events;
- every account's postings summing to its balance, at its version;
- every payment, refund and payout in the same state, at the same version;
- every merchant's captured total.

After a day of simulated traffic and every chaos run: 169,601 ledger events, 110,940 payment events,
1,973 accounts and 29,199 payments, refunds and payouts, with no difference. That includes ClickHouse
itself killed twice mid-ingestion (`kill-clickhouse`, in the table [below](#chaos)). Payment clients
didn't notice it was gone: analytics hangs off Kafka, not off the payment path. Events reach ClickHouse
about 0.5 s after their commit (p95 1 s).

**The dashboard has no API server of its own.** ClickHouse serves a fixed set of read-only queries at
their own URLs (`/api/kpis`, `/api/throughput`, ...: [`api.xml`](deploy/clickhouse/api.xml)), each taking
only a time window. They run as a read-only user limited to the analytics database, with a 10 s cap,
and answer in 5–30 ms. Caddy serves the Vue app and forwards just those GET paths. Any other path, any
other method, and ClickHouse's own query interface get a 404 from outside.

**What it shows**, in a window of 15 minutes to a day, refreshed every 5 s:

- card payments by outcome: approved, declined as fraud, declined for other reasons;
- money captured;
- the money in flight: card holds, and payouts on their way to other banks;
- the fraud scores, by decision;
- the model's reasons for its declines, in words;
- payouts by outcome, with how long they took;
- the top merchants;
- the books from ClickHouse alone: by kind of account, summing to zero;
- the pipeline's lag.

Each chart can be shown as a table. Both themes are designed: the colours are a validated palette, in
fixed order, with light and dark steps chosen separately
([dark](docs/screenshots/dashboard-dark.png)). It works down to a phone's width.

## Merchants

Merchants use a gRPC API ([`merchants.proto`](proto/ledgerline/v1/merchants.proto)), served by the payments
service on its own port:

```
Authorise   CapturePayment   VoidPayment   RefundPayment   GetPayment   ListPayments
WatchPayments   a live stream of every change to the merchant's payments and refunds
```

- **Each merchant is on its own.**
  - It has an API key (`authorization: Bearer ll_sk_...`, stored only as a SHA-256) and a merchant
    account in the ledger.
  - It is the payee of everything it authorises, and sees nothing else: another merchant's payment is
    `NOT_FOUND`, not "forbidden", which would say that it exists.
- **The same writes as the HTTP API.** Both go through one code path, so a write over gRPC has the same
  idempotency and the same steps.
  - The key comes as metadata (`idempotency-key`), and is the merchant's own: two shops using `order-1`
    don't collide.
  - A replay is marked by the `idempotent-replayed` header.
  - A refusal is a gRPC status plus a stable code in the `ledgerline-error-code` trailer
    (`insufficient_funds`, `invalid_state`, ...).
- **The live stream comes from Kafka.** A cursor over the database's event log would skip an event whose
  transaction commits after a later one's. Kafka's offsets have no such gaps. Each event carries a
  cursor to resume from.

**Webhooks**, for every change to a merchant's payments and refunds:

- **Queued with the change.** A delivery is written in the same database transaction as the change it
  describes, so it is sent if and only if the change commits.
- **Signed.** `Ledgerline-Signature: t=<time>,v1=<HMAC-SHA256 of "t.body">`, keyed by the merchant's
  secret. A receiver checks the signature and that `t` is recent, so a recorded request can't be
  replayed.
- **Retried.** Anything but a 2xx is tried again: after 1 s, doubling, up to 10 minutes apart, 12 times.
  After that the delivery is kept as failed and can be redelivered. Each event carries its id
  (`stream:version`), so a receiver can skip one it has seen.

**The SDK** ([`sdk`](sdk), TypeScript, Node 24):

```ts
const ledgerline = new Ledgerline({ target: "payments:9090", apiKey });
const payment = await ledgerline.authorise({ customer, amount: 12_500 }, { idempotencyKey: orderId });
await ledgerline.capture(payment.id);
for await (const event of ledgerline.watch()) { ... }        // resumes by itself if the stream breaks
const event = verifyWebhook(rawBody, req.headers["ledgerline-signature"], webhookSecret);
```

- **Safe retries.** A write that gets no answer is sent again with the same idempotency key, backing off
  with jitter, for up to 30 s, which is long enough to ride out a restart of the service.
- **Signatures agree across languages.** Webhook verification is constant-time. The server (Kotlin) and
  the SDK are tested against the same signature value, so they can't drift apart.

**End to end, while things break** ([`chaos/merchant.sh`](chaos/merchant.sh),
[`eval/merchant`](eval/merchant)): an example shop ([`shop.ts`](sdk/examples/shop.ts)) took card payments
through the SDK for a minute.

- **The faults:** its own webhook endpoint refused everything for the first 30 s, and the payments
  service was killed at 25 s and came back at 35 s, mid-payment.
- **The orders:** 232, with no errors. The SDK rode out the restart, retrying with the same keys.
- **Every change was heard.** All 1,085 changes Ledgerline recorded for the shop's payments and refunds
  arrived as signed webhooks: none missing and no bad signatures, after 1,609 deliveries had been
  refused during the outage. All 1,085 also came through the live stream.
- **The books held:** the reconciliation and the analytics check passed.

## Tested to break

The tests run against a real PostgreSQL and Kafka. The payments tests run a real ledger and the
simulated bank in the same JVM.

- **The ledger against a model:** Kotest generates 150 random sequences of up to 40 operations:
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
- **The outbox, end to end:** 2,000 concurrent transfers, then every account rebuilt from Kafka. Versions
  arrive in order, with no gaps, and every balance equals the ledger's.
- **Lost answers:** the payments service talks to a ledger whose answers get lost. A fifth of the calls
  fail before reaching it. Another fifth reach it and take effect, and then the answer is lost: the
  money moved, and the caller doesn't know. 240 random payments, captures, voids, refunds and payouts
  run against it at once (in the last run, 215 answers were lost after the ledger acted). When it all
  settles:
  - every customer's and merchant's balance is exactly what the final states imply;
  - the ledger's audit passes;
  - the reconciliation holds.
- **A bank that misbehaves:** 150 payouts at once against a bank that fails 30% of its answers, answers
  20% too late, and returns 20% after accepting them. Every payout ends exactly once, settled or
  returned (in the last run: 121 settled, 14 returned by the bank after accepting them, 15 rejected for
  closed accounts). The bank's records match
  ours, and only settled payouts left the customers' accounts.
- **The tests catch what they claim to.** With deliberate bugs put in:
  - with the row lock removed, both ledger race tests fail;
  - with a new idempotency key on each try, the lost answers make captures run twice. The ledger
    refuses the second (holds can't go negative), and the lost-answers test fails;
  - with money given back when the bank's answer is unknown, the reconciliation finds 22 payouts the
    bank paid that were also refunded.
- **The API:** authorisations, partial captures, voids, expiry, refunds, refusals and idempotent
  replays; transfers, reversals, statements and paging; an append-only event log; a passing audit.

## Chaos

The tests above break things from inside a test. [`chaos/run.sh`](chaos/run.sh) breaks the running
system, while a client keeps paying.

- **The client** (`checker workload`) runs card payments and payouts at 15 flows a second for two
  minutes: authorise, then capture all or part, void or leave; refunds; payouts. It behaves as a client
  should:
  - every write has an idempotency key;
  - a request that gets no answer (a timeout, a refused connection, a 5xx, a 503) is sent again with the
    same key, backing off, until it gets one;
  - a tenth of its requests are sent twice at once.

  It logs every request and every answer.
- **The faults**, on a schedule, while the client runs:
  - `kill -9` of the ledger, the payments service, PostgreSQL or Kafka;
  - a service cut off the network;
  - Kafka's data deleted.

  [Toxiproxy](https://github.com/Shopify/toxiproxy) sits between the payments service and the ledger
  and the bank. It slows the connections, cuts answers off after 150 bytes (the posting is made and the
  answer is lost), resets them, or holds them open and silent.
- **The proof** (`checker verify`), once everything is back:
  - every request got an answer in the end, and all the answers for one key named the same resource;
  - the payments, refunds and payouts the service has are exactly those the client was told of;
  - each one has exactly the ledger postings its final state implies, once each. A captured payment
    has one authorisation and one capture; a returned payout has one reservation and one return; a
    declined payment has none.
  - every balance is what those states imply;
  - the reconciliation and the ledger's audit hold;
  - both services' books rebuild from Kafka alone.

| Scenario | What breaks | Requests (sent twice at once) | Failed tries, retried | Time to an answer: p50 / p95 / max | Verified |
| --- | --- | --- | --- | --- | --- |
| `baseline` | nothing | 3,691 (378) | 0 | 11 ms / 17 ms / 121 ms | all checks hold |
| `kill-ledger` | the ledger killed (SIGKILL) 3 times, 10 s each | 3,698 (396) | 647 | 435 ms / 2.2 s / 28.9 s | all checks hold |
| `kill-payments` | the payments service killed 3 times, mid-step | 3,700 (370) | 18,820 | 2.2 s / 87.5 s / 137.0 s | all checks hold |
| `kill-postgres` | PostgreSQL killed twice: both services lose their database | 3,667 (384) | 2,397 | 13 ms / 10.7 s / 23.9 s | all checks hold |
| `kill-fraud` | the fraud scorer (enforcing) killed twice, 15 s each | 3,675 (386) | 249 | 12 ms / 488 ms / 5.3 s | all checks hold |
| `kill-clickhouse` | ClickHouse killed twice mid-ingestion, 15 s each | 3,619 (354) | 0 | 12 ms / 21 ms / 84 ms | all checks hold, and analytics agrees |
| `kill-kafka` | Kafka killed for 50 s | 3,691 (379) | 0 | 10 ms / 17 ms / 60 ms | all checks hold |
| `partition` | the ledger, then the bank, cut off the network for 20 s | 3,696 (369) | 71 | 10 ms / 2.0 s / 19.5 s | all checks hold |
| `toxic` | connections to the ledger slowed, cut mid-answer, reset, hung; the bank's hung | 3,661 (405) | 492 | 15 ms / 2.2 s / 16.5 s | all checks hold |
| `kafka-loss` | Kafka loses all its data; refilled from the event logs | 3,730 (386) | 18,444 | 1.3 s / 91.2 s / 128.3 s | all checks hold |
| `mixed` | PostgreSQL, Kafka and payments killed; answers cut | 3,633 (381) | 4,866 | 45 ms / 23.5 s / 44.7 s | all checks hold |

Over 40,461 requests, 4,188 of them sent twice at once, nothing was lost and nothing was done twice
([`eval/chaos`](eval/chaos): each scenario's report, the client's summary and the faults' timeline).

- **A dead ledger** is hidden from the payments service's clients. Its account facts are cached, so an
  authorisation is recorded at once (202), and the worker posts it when the ledger is back.
- **A dead Kafka** isn't noticed at all: the outboxes grow, then drain.
- **A dead PostgreSQL** is an outage for both services, of about the 10 s it takes to come back.
- **Answers cut off mid-way** are the case the idempotency keys exist for: the posting was made, the
  answer lost, and the retry lands on the first try's result.
- **Kafka losing everything**, the incident from stage 2, is now one scenario among the others. With the
  services stopped, `checker republish` put 498,042 ledger events and 319,931 payment events back in the
  outboxes, in 8 s and 10 s, and the relays sent them again. Writes were down for 47 s in all.
- **At least once, in practice:** in one run, killing the payments service between a batch's send and
  its delete made the relay send 20 events twice. The checker, like any consumer, skipped them.

The slow answers in `kill-payments` and `kafka-loss` are the price of outages, not of the design. In two
minutes the payments service is down three times (in `kafka-loss`, writes are down for 47 s), and the
clients' backlog meets a service that has just restarted. The run also found three things to fix:

- **Slow restarts.** On one CPU the services took 14–15 s to start. They now start from a Java 25
  ahead-of-time cache, made by a training run when the image is built, and take 7–8 s.
- **A client that wasn't patient enough.** Waiting for a 202 to finish, it asked again every 200 ms, so
  after an outage hundreds of clients' reads crowded out the writes. Now it backs off.
- **Unanswered requests, the client's fault.** Requests sent twice near the end of a run were dropped,
  because their executor had been shut down. They were the client's bug, and the proof caught them.

## Measured

k6 load tests on a 4-core host shared with other work. The ledger, the payments service, PostgreSQL and
Kafka each run on 1 CPU, and k6 itself on 1 more.

### The ledger

[`load/ledger.js`](load/ledger.js) runs transfers between 1,000 accounts at rising rates, then all into
5 merchant accounts (every transfer locks a merchant's row: the worst case for contention). These are
stage 1's results, before the outbox ([`eval/load-ledger.json`](eval/load-ledger.json)):

| Transfers/s asked | Done/s | Shed (fast 503) | p50 | p95 | p99 | Failed |
| --- | --- | --- | --- | --- | --- | --- |
| 100 | 100 | 0% | 4 ms | 10 ms | 37 ms | 0% |
| 250 | 250 | 0% | 3 ms | 11 ms | 42 ms | 0% |
| 500 | 440 | 12% | 237 ms | 332 ms | 392 ms | 0% |
| 1,000 | 429 | 57% | 205 ms | 326 ms | 401 ms | 0% |
| 250, all into 5 merchants | 250 | 0.02% | 3 ms | 10 ms | 71 ms | 0% |

- **Capacity:** about 430–440 transfers a second, with both CPUs at their caps. Beyond that it refuses
  the excess quickly and keeps serving what it can.
- **Audit:** afterwards it passed over all 152,946 transactions and 460,850 events.
- **The outbox's cost:** every event is now also written to the outbox and sent to Kafka. The ledger
  manages about **380 transfers a second** (asked for 400, 5% shed), with p95 53 ms at 250 a second
  ([`eval/load-ledger-outbox.json`](eval/load-ledger-outbox.json)).

The first run of this test found the reason for the shedding. With virtual threads every request gets a
thread at once, so an overload doesn't queue in the web server: it piles up inside the service. At
1,000 a second, 1,500 requests were waiting for 16 database connections when the heap ran out and the
JVM died. The books were intact even then (the audit passed afterwards), but a ledger mustn't fall over
under load.

### Payments

[`load/payments.js`](load/payments.js) sends card payments at rising rates, 45 s each, with 5 payouts a
second to the misbehaving bank alongside. A card payment is an authorisation, then (mostly) a capture:
two requests to the payments service, two ledger postings, writes to two databases and two outboxes
([`eval/load-payments.json`](eval/load-payments.json)).

| Card payments/s asked | Authorised/s | Captured/s | Shed | Authorise p50 / p95 / p99 | Capture p50 / p95 | Failed |
| --- | --- | --- | --- | --- | --- | --- |
| 25 | 25 | 24 | 0 | 12 / 28 / 46 ms | 11 / 23 ms | 0% |
| 50 | 50 | 48 | 0 | 11 / 104 / 172 ms | 12 / 112 ms | 0% |
| 75 | 71 | 65 | 276 | 348 / 500 / 600 ms | 379 / 531 ms | 0% |
| 100 | 76 | 65 | 1,408 | 362 / 496 / 595 ms | 395 / 526 ms | 0% |

- **Capacity:** about 70–75 card payments a second, plus the payouts.
- **The limit is PostgreSQL:** one CPU serves both services' databases, and it was throttled for 152 s
  of the run.
- **Afterwards:** the reconciliation held over 67,163 payments and 4,106 payouts, the ledger's audit
  passed, and the checker rebuilt every account from Kafka.

Measuring found five problems, each fixed:

- **One holds account per currency was the hottest row in the system.** Every authorisation and capture
  posts to it, and a posting locks its accounts' rows, so every card payment waited for every other.
  The lock query averaged 23 ms per call. A backend that was CPU-throttled while holding the lock made
  everyone behind it wait. Now there are 16 holds and 16 clearing accounts per currency, and each
  payment or payout keeps the one it was given.
- **The outbox table had bloated.** After the backfill of 690,000 events it was 19 MB with 89 live
  rows, and the relay's reads and deletes scanned it 20 times a second. It was shrunk once, and
  autovacuum now cleans it after every thousand deletions.
- **Each authorisation asked the ledger about the two accounts:** their currency and kind, which never
  change. The answers are now kept.
- **Cold JVMs:** a JVM on one CPU spends minutes compiling before it is fast, so it is measured only
  after a warm-up.
- **The simulated bank couldn't write its journal** (its volume was owned by root). It accepted 2,027
  payouts and never settled them.
  - The sagas did what they should: they kept the money reserved and kept asking.
  - Once the bank was fixed, it no longer knew those payouts. The sagas sent them again by the same ids.
  - All 2,027 settled or were returned, and the reconciliation held.

And one lesson, the hard way. Kafka was first set up keeping its data inside its container, and
recreating the container lost the topics.

- **The ledger recovered completely.** It sent its whole history again from its event log, and the
  checker rebuilt all 1,059,836 events and 8,748 accounts from Kafka.
- **The payments service couldn't.** It kept only the current state, not an event log, so its earlier
  events were gone: Kafka is not a system of record.
- **Fixed in stage 3:** the payments service now has an event log of its own, and
  [Kafka losing its data](#chaos) is one of the chaos scenarios.

## Running it

```bash
cp deploy/.env.example deploy/.env      # set the passwords (openssl rand -hex 24)
docker compose -f deploy/compose.yaml --env-file deploy/.env up -d --build
docker compose -f deploy/compose.yaml --env-file deploy/.env run --rm checker   # rebuild the books from Kafka
```

A chaos run, against the stack above (with Toxiproxy added by `deploy/chaos.yaml`):

```bash
chaos/run.sh kill-payments          # or baseline, kill-ledger, kill-postgres, kill-kafka, partition, toxic, kafka-loss, mixed
python chaos/table.py               # the table above, from eval/chaos
```

Fraud: collect in shadow mode, train, then enforce and measure (in the `ledgerline-fraud` image, on the
Compose network, with `KAFKA=kafka:9092`):

```bash
python -m fraud.sim                                   # MINUTES, SEED, OUT=/out/truth.jsonl
python -m fraud.train --truth /out/truth.jsonl --model-dir /model --report /out/offline.json
python -m fraud.evaluate /out/truth-live.jsonl        # after a run with FRAUD_MODE=enforce
python -m fraud.parity /out/truth-live.jsonl          # online against offline
```

The dashboard listens on the host's loopback only (port 18480): `ssh -L 18480:127.0.0.1:18480 <host>`, then
open `http://localhost:18480`. Whether ClickHouse agrees with the databases:
`docker compose -f deploy/compose.yaml --env-file deploy/.env run --rm checker analytics`.

A merchant, end to end: `chaos/merchant.sh` (it builds on the `ledgerline-shop` image: `docker build -f
sdk/Dockerfile -t ledgerline-shop .`). A merchant of your own: `POST /internal/merchants {name, webhookUrl}`
on the payments service gives its API key and webhook secret, once.

The tests, with a throwaway PostgreSQL and Kafka in Docker:

```bash
scripts/test-in-docker.sh
```

Or with your own: `LEDGERLINE_TEST_DB=jdbc:postgresql://host:5432 LEDGERLINE_TEST_KAFKA=host:9092 ./gradlew build`
(with the databases `ledger_test` and `payments_test`).

| Module | |
| --- | --- |
| [`ledger`](ledger) | the ledger service |
| [`payments`](payments) | card payments, refunds, payouts; the worker; reconciliation |
| [`platform`](platform) | shared by the services: the outbox and its relay, load shedding |
| [`otherbank`](otherbank) | the simulated other bank |
| [`checker`](checker) | the tools: rebuild the books from Kafka and compare (`check`), refill Kafka from the event logs (`republish`), the chaos client (`workload`) and its proof (`verify`) |
| [`chaos`](chaos/run.sh) | the chaos runs |
| [`fraud`](fraud) | Python: the scorer, its features, the simulator, training, evaluation and the online/offline parity check |
| [`dashboard`](dashboard) | Vue 3 and Chart.js, served by Caddy |
| [`proto`](proto/ledgerline/v1/merchants.proto), [`api`](api) | the merchants' gRPC API: its contract, and the Java code generated from it |
| [`sdk`](sdk) | TypeScript: the merchants' SDK, its tests, and an example shop |
| [`deploy/clickhouse`](deploy/clickhouse) | the analytics schema (Kafka engine, materialized views) and the dashboard's endpoints |

The stack is Kotlin 2.3, Spring Boot 4.1 (Spring MVC on virtual threads, JdbcClient, Flyway), Java 25,
PostgreSQL 18 and Kafka 4.4 (KRaft, one broker); the fraud scorer is Python 3.13 (FastAPI, LightGBM,
confluent-kafka); analytics is ClickHouse 26.8 and a Vue 3.5 dashboard (TypeScript, Chart.js); merchants use gRPC (grpc-java 1.85,
protobuf 4.36) and a TypeScript SDK (Node 24, grpc-js).

## Roadmap

| Stage | |
| --- | --- |
| 1. Ledger core | done |
| 2. Payments | done: card payments, refunds and payouts as sagas; an outbox into Kafka |
| 3. Chaos | done: services, Kafka and the database killed, networks cut and answers lost mid-payment, with a proof that nothing was lost or done twice; an event log for the payments service |
| 4. Fraud | done: a simulator with known fraud, real-time features from Kafka, a model scored against the truth, in-line and failing open |
| 5. Analytics | done: ClickHouse fed from Kafka, proven to agree with the ledger; a Vue 3 dashboard |
| 6. Merchants | done: a gRPC API, signed webhooks, a TypeScript SDK, proven end to end while things break |
| 7. Production | OpenTelemetry tracing, Grafana, alerts |
| 8. Publishing | Demo, write-up |
