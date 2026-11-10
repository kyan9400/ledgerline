-- Analytics: ClickHouse reads both topics through its Kafka engine, and materialized views turn the
-- events into tables. Delivery is at least once (the Kafka engine commits after the views have written;
-- the relays may send twice), so every table is a ReplacingMergeTree keyed by the event's identity, and
-- queries read it with FINAL: an event counts once, however often it arrived.
--
-- Runs once, when ClickHouse first initialises its volume.

CREATE DATABASE IF NOT EXISTS analytics;

-- --- the ledger ------------------------------------------------------------------------------------

CREATE TABLE analytics.ledger_queue (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'ledger.events', kafka_group_name = 'clickhouse-ledger',
         kafka_format = 'JSONAsString', kafka_num_consumers = 1, kafka_poll_timeout_ms = 500, kafka_flush_interval_ms = 1000;

CREATE TABLE analytics.ledger_events (
    stream      String,
    version     UInt32,
    type        LowCardinality(String),
    recorded_at DateTime64(6, 'UTC'),
    data        String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree
ORDER BY (stream, version);

CREATE MATERIALIZED VIEW analytics.ledger_events_mv TO analytics.ledger_events AS
SELECT JSONExtractString(raw, 'stream') AS stream,
       toUInt32(JSONExtractUInt(raw, 'version')) AS version,
       JSONExtractString(raw, 'type') AS type,
       parseDateTime64BestEffort(JSONExtractString(raw, 'recordedAt'), 6, 'UTC') AS recorded_at,
       JSONExtractRaw(raw, 'data') AS data
FROM analytics.ledger_queue;

-- Every posting to every account: the balances are their sums.
CREATE TABLE analytics.postings (
    account       UUID,
    version       UInt32,
    transaction   UUID,
    amount        Int64,
    balance_after Int64,
    recorded_at   DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree
ORDER BY (account, version);

CREATE MATERIALIZED VIEW analytics.postings_mv TO analytics.postings AS
SELECT toUUID(substring(stream, 9)) AS account, version,
       toUUID(JSONExtractString(data, 'transaction')) AS transaction,
       JSONExtractInt(data, 'amount') AS amount,
       JSONExtractInt(data, 'balanceAfter') AS balance_after,
       recorded_at
FROM analytics.ledger_events WHERE type = 'Posted';

CREATE TABLE analytics.accounts (
    account   UUID,
    code      String,
    kind      LowCardinality(String),
    currency  LowCardinality(String),
    opened_at DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree
ORDER BY account;

CREATE MATERIALIZED VIEW analytics.accounts_mv TO analytics.accounts AS
SELECT toUUID(substring(stream, 9)) AS account,
       JSONExtractString(data, 'code') AS code,
       JSONExtractString(data, 'kind') AS kind,
       JSONExtractString(data, 'currency') AS currency,
       recorded_at AS opened_at
FROM analytics.ledger_events WHERE type = 'AccountOpened';

-- --- payments, refunds, payouts --------------------------------------------------------------------

CREATE TABLE analytics.payments_queue (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'payments.events', kafka_group_name = 'clickhouse-payments',
         kafka_format = 'JSONAsString', kafka_num_consumers = 1, kafka_poll_timeout_ms = 500, kafka_flush_interval_ms = 1000;

CREATE TABLE analytics.payment_events (
    stream      String,
    version     UInt32,
    type        LowCardinality(String),
    recorded_at DateTime64(6, 'UTC'),
    data        String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree
ORDER BY (stream, version);

CREATE MATERIALIZED VIEW analytics.payment_events_mv TO analytics.payment_events AS
SELECT JSONExtractString(raw, 'stream') AS stream,
       toUInt32(JSONExtractUInt(raw, 'version')) AS version,
       JSONExtractString(raw, 'type') AS type,
       parseDateTime64BestEffort(JSONExtractString(raw, 'recordedAt'), 6, 'UTC') AS recorded_at,
       JSONExtractRaw(raw, 'data') AS data
FROM analytics.payments_queue;

-- Each payment, refund and payout as it is now: the row with the highest version wins.
CREATE TABLE analytics.latest (
    entity          LowCardinality(String),  -- payment, refund, payout
    id              UUID,
    version         UInt32,
    state           LowCardinality(String),
    customer        UUID,                    -- payments
    merchant        UUID,                    -- payments
    account         UUID,                    -- payouts
    payment         UUID,                    -- refunds
    currency        LowCardinality(String),
    amount          Int64,
    captured        Int64,
    refunded        Int64,
    decline_reason  LowCardinality(String),
    failure_reason  LowCardinality(String),
    fraud_score     Nullable(Float64),
    fraud_decision  LowCardinality(String),
    fraud_reasons   String,
    created_at      DateTime64(6, 'UTC'),
    updated_at      DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(version)
ORDER BY (entity, id);

CREATE MATERIALIZED VIEW analytics.latest_mv TO analytics.latest AS
SELECT substring(stream, 1, position(stream, '-') - 1) AS entity,
       toUUID(JSONExtractString(data, 'id')) AS id,
       version,
       JSONExtractString(data, 'state') AS state,
       toUUIDOrZero(JSONExtractString(data, 'customer')) AS customer,
       toUUIDOrZero(JSONExtractString(data, 'merchant')) AS merchant,
       toUUIDOrZero(JSONExtractString(data, 'account')) AS account,
       toUUIDOrZero(JSONExtractString(data, 'payment')) AS payment,
       JSONExtractString(data, 'currency') AS currency,
       JSONExtractInt(data, 'amount') AS amount,
       JSONExtractInt(data, 'captured') AS captured,
       JSONExtractInt(data, 'refunded') AS refunded,
       JSONExtractString(data, 'declineReason') AS decline_reason,
       JSONExtractString(data, 'failureReason') AS failure_reason,
       JSONExtract(data, 'fraudScore', 'Nullable(Float64)') AS fraud_score,
       JSONExtractString(data, 'fraudDecision') AS fraud_decision,
       JSONExtractString(data, 'fraudReasons') AS fraud_reasons,
       parseDateTime64BestEffort(JSONExtractString(data, 'createdAt'), 6, 'UTC') AS created_at,
       parseDateTime64BestEffort(JSONExtractString(data, 'updatedAt'), 6, 'UTC') AS updated_at
FROM analytics.payment_events;
