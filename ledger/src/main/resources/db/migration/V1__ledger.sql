-- The ledger. The event log is the source of truth: it is only ever appended to. Balances, entries and
-- transactions are projections of it, written in the same database transaction as the events, and can
-- be rebuilt from them (GET /internal/audit does, and compares).

CREATE TABLE accounts (
    id             uuid PRIMARY KEY,
    code           text NOT NULL UNIQUE CHECK (code ~ '^[a-z0-9][a-z0-9:_.-]{0,99}$'),
    currency       char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    kind           text NOT NULL CHECK (kind IN ('customer', 'merchant', 'system')),
    -- Only system accounts (issuance, settlement with other banks) may go below zero.
    allow_negative boolean NOT NULL DEFAULT false CHECK (NOT allow_negative OR kind = 'system'),
    opened_at      timestamptz NOT NULL DEFAULT now()
);

-- Every change, in order. A stream is one account's history ("account-<id>") or one transaction
-- ("transaction-<id>"); version is the position in it, from 1, so two writers can't both append the
-- same next event.
CREATE TABLE events (
    seq         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream      text NOT NULL,
    version     integer NOT NULL CHECK (version >= 1),
    type        text NOT NULL,
    data        jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (stream, version)
);

CREATE FUNCTION events_are_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'the event log is append-only (% refused)', TG_OP;
END
$$;
CREATE TRIGGER events_append_only BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_are_immutable();
CREATE TRIGGER events_no_truncate BEFORE TRUNCATE ON events
    FOR EACH STATEMENT EXECUTE FUNCTION events_are_immutable();

-- Projections.
CREATE TABLE balances (
    account_id     uuid PRIMARY KEY REFERENCES accounts (id),
    balance        bigint NOT NULL,
    version        integer NOT NULL,  -- the account stream's last version
    allow_negative boolean NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    -- The service checks funds before it writes; this is the backstop if it ever didn't.
    CONSTRAINT no_overdraft CHECK (allow_negative OR balance >= 0)
);

CREATE TABLE transactions (
    id          uuid PRIMARY KEY,
    kind        text NOT NULL CHECK (kind IN ('posting', 'transfer', 'reversal')),
    reverses    uuid UNIQUE REFERENCES transactions (id),  -- a transaction is reversed at most once
    description text NOT NULL DEFAULT '',
    metadata    jsonb NOT NULL DEFAULT '{}',
    recorded_at timestamptz NOT NULL
);

CREATE TABLE entries (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id  uuid NOT NULL REFERENCES transactions (id),
    account_id      uuid NOT NULL REFERENCES accounts (id),
    amount          bigint NOT NULL CHECK (amount <> 0),  -- minor units: kopecks, cents
    balance_after   bigint NOT NULL,
    account_version integer NOT NULL,
    recorded_at     timestamptz NOT NULL,
    UNIQUE (account_id, account_version)
);
CREATE INDEX entries_transaction ON entries (transaction_id);

-- A request retried with the same key gets the first answer, whatever it was (a refusal for lack of
-- funds included); the same key with a different request is refused.
CREATE TABLE idempotency_keys (
    key          text PRIMARY KEY CHECK (length(key) BETWEEN 1 AND 200),
    request_hash text NOT NULL,
    status       integer NOT NULL,
    response     text NOT NULL,  -- the exact bytes sent, so a replay is identical
    created_at   timestamptz NOT NULL DEFAULT now()
);
