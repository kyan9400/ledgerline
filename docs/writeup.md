# Ledgerline: money that can't be lost, and how that was checked

Ledgerline is a payments platform built the way a bank's is: a double-entry ledger, card payments and
payouts run as sagas on top of it, events into Kafka, fraud scoring in line, analytics, a merchants' API,
and the monitoring to run it. This is the story of its design decisions, and of what breaking it on
purpose found. Every number here is measured, and the [README](../README.md) links to the raw results.

## The one property

A payments system can be slow, or down, and recover. It can't create money, lose it, or move it twice.
Everything else follows from that, and each of these is a design decision with a test behind it.

- **The ledger is an append-only event log.** Balances are a projection, written in the same transaction
  and rebuildable from the events; a trigger refuses to change the log. A property test runs 150 random
  sequences of operations against the ledger and a few-line model, and they must agree after every step.
- **Every write is idempotent.** The key is claimed in the write's own transaction, so a retry gets the
  first answer byte for byte. This is what makes every other recovery safe: a request that got no
  answer can always be sent again.
- **A saga never guesses.** A timeout at the bank says nothing about whether the bank got the payout, so
  the money stays reserved and the payout is sent again by the same id until the bank answers. When a
  bug made the simulated bank forget 2,027 payouts, the sagas kept them reserved, sent them again once it
  was fixed, and every one settled or came back.
- **Events leave through an outbox,** in the same transaction as the change, so an event is published if
  and only if its change commits. Kafka is not the system of record: when it lost its data (once by
  accident, then on purpose), each service's event log refilled it.

## Proving it under failure

Tests that break things from inside a test are not enough, so the running system is broken while a
client pays: services killed with SIGKILL mid-step, PostgreSQL killed, Kafka killed or wiped, networks
cut, answers cut off after 150 bytes (the posting made, the answer lost). Then a checker proves, from
the client's log, the databases and Kafka alone, that every request got an answer, every payment has
exactly the postings its state implies, and every balance follows.

Over 11 scenarios and 40,461 requests (4,188 of them sent twice at once), nothing was lost and nothing
was done twice. The runs also found what tests hadn't: services too slow to restart on one CPU (now a
Java 25 ahead-of-time cache halves it), a client whose polling crowded out writes after an outage, and a
client bug the proof caught.

## Performance, and the hot row

Load tests on one CPU per service found the limits and the reasons for them. The ledger does about 430
transfers a second; beyond that it sheds the excess with a fast 503 rather than falling over (the first
run of that test killed the JVM: virtual threads let 1,500 requests pile up behind 16 connections). Card
payments reached 70 to 75 a second, limited by PostgreSQL. The worst finding was a single holds account
per currency: every authorisation locked the same row. It is sharded 16 ways now.

## Fraud, measured against the truth

A simulator drives the real API with customers who have habits, and with stolen cards in three
patterns. It writes down the truth where nothing in the system can see it. A LightGBM model on features
the scorer keeps from Kafka caught 88% of fraud at a 0.7% false-decline rate, against 37% for rules tuned
to the same budget; live, enforcing, it stopped 91.9%. Live also showed the cost: 1.6% of genuine
payments declined, more than offline, because the threshold rested on 14 genuine examples. That is
written down as found, with the fix (a longer shadow period). The scorer fails open after 150 ms: a
fraud check that can stop every payment is a bigger outage than the fraud.

## Analytics that agree to the kopeck

ClickHouse reads both topics through its Kafka engine. Delivery is at least once, so every table is keyed
by the event's identity and read deduplicated. A checker compares it with the ledger, account by account
and payment by payment: after a day of traffic and every chaos run, no difference. The dashboard has no
API server: ClickHouse serves a fixed set of read-only queries, and Caddy forwards only those.

## Merchants

A gRPC API, with the same write path (and idempotency) as the HTTP one; signed webhooks, queued in the
same transaction as the change; a live event stream from Kafka, resumable by cursor; and a TypeScript
SDK that retries with the same key. End to end, with the merchant's webhook endpoint refusing for 30 s
and the payments service killed mid-payment: 232 orders, no errors, and all 1,085 changes heard, by
webhook and by stream.

## Running it: traces that follow the money

Much of what happens to a payment happens later and elsewhere: the outbox sends its events, the worker
finishes a step the request couldn't, a webhook goes out after retries. So each row acted on later keeps
the trace context of the change that wrote it, and whoever picks it up continues the trace. A payout's
trace runs through five worker steps, the bank and the ledger; a payment authorised while the ledger was
down shows each failed attempt and the one that worked.

The alerts page on SLO burn rates, and on what those can't see: a payment held up by a dead ledger still
gets its answer (202), so the work waiting is watched too, and the ledger audits its own books every ten
minutes. To know the alerts work, each fault was injected under load: every one was caught by the alert
meant for it, and a run with nothing broken paged nobody. The first runs found a metric that lied (the
outbox's lag read 0 while Kafka was down), an outage nothing paged for (the database), and a slowdown
the SLO arithmetic forgave; each is fixed, and the evaluation is in the repository to run again.

## What it is not

One host, one broker, one database server, made-up money and customers. The design goes further than
the deployment: the relays and workers take leases and locks so several instances can share the work,
but it has only ever run one of each.
