-- The payments service's own database. Money itself lives in the ledger; this keeps what each payment,
-- refund and payout is doing, as a state machine. A state ending in -ing is a step in progress: it has a
-- ledger (or bank) call to make, and next_attempt_at says when the worker should (re)try it. Each step's
-- idempotency key is fixed by the payment and the step (payments:payment:<id>:capture), so a retry, from
-- the worker or after a crash, can't move money twice. Every change of state bumps version and writes an
-- event to the outbox.

CREATE TABLE payments (
    id              uuid PRIMARY KEY,
    customer        uuid NOT NULL,         -- ledger accounts
    merchant        uuid NOT NULL,
    currency        char(3) NOT NULL,
    amount          bigint NOT NULL CHECK (amount > 0),  -- authorised, in minor units
    state           text NOT NULL CHECK (state IN
                      ('authorising', 'authorised', 'declined', 'capturing', 'captured', 'voiding', 'voided')),
    captured        bigint NOT NULL DEFAULT 0,
    refunded        bigint NOT NULL DEFAULT 0,  -- refunds succeeded or in progress
    capture_amount  bigint,                      -- while capturing
    decline_reason  text,
    void_reason     text,                        -- requested, expired
    description     text NOT NULL DEFAULT '',
    version         integer NOT NULL,
    attempts        integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_error      text,
    expires_at      timestamptz,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    CHECK (captured <= amount),
    CHECK (refunded <= captured),
    CHECK (capture_amount IS NULL OR capture_amount BETWEEN 1 AND amount)
);
CREATE INDEX payments_due ON payments (next_attempt_at) WHERE next_attempt_at IS NOT NULL;
CREATE INDEX payments_expiring ON payments (expires_at) WHERE state = 'authorised';

CREATE TABLE refunds (
    id              uuid PRIMARY KEY,
    payment_id      uuid NOT NULL REFERENCES payments (id),
    amount          bigint NOT NULL CHECK (amount > 0),
    state           text NOT NULL CHECK (state IN ('refunding', 'refunded', 'failed')),
    failure_reason  text,
    version         integer NOT NULL,
    attempts        integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_error      text,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);
CREATE INDEX refunds_payment ON refunds (payment_id);
CREATE INDEX refunds_due ON refunds (next_attempt_at) WHERE next_attempt_at IS NOT NULL;

-- A payout to an account at another bank, as a saga:
--   reserving  -> submitting (money held in clearing) | refused (not enough money)
--   submitting -> submitted (the bank has it) | returning (the bank refused it)
--   submitted  -> settling (the bank settled it) | returning (the bank returned it)
--   settling   -> settled;  returning -> returned (money back to the customer)
CREATE TABLE payouts (
    id                  uuid PRIMARY KEY,
    account             uuid NOT NULL,
    currency            char(3) NOT NULL,
    amount              bigint NOT NULL CHECK (amount > 0),
    bank                text NOT NULL,
    beneficiary         text NOT NULL,
    state               text NOT NULL CHECK (state IN
                          ('reserving', 'refused', 'submitting', 'submitted', 'settling', 'settled', 'returning', 'returned')),
    accepted_by_bank    boolean NOT NULL DEFAULT false,
    failure_reason      text,
    description         text NOT NULL DEFAULT '',
    version             integer NOT NULL,
    attempts            integer NOT NULL DEFAULT 0,
    next_attempt_at     timestamptz,
    last_error          text,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL
);
CREATE INDEX payouts_due ON payouts (next_attempt_at) WHERE next_attempt_at IS NOT NULL;

-- A request retried with the same key gets the answer stored for the first. A step still in progress
-- when the first answer was sent shows as such (202); GET the resource for its state now.
CREATE TABLE idempotency_keys (
    key          text PRIMARY KEY CHECK (length(key) BETWEEN 1 AND 200),
    request_hash text NOT NULL,
    status       integer NOT NULL,
    response     text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- The transactional outbox (dev.ledgerline.platform.Outbox).
CREATE TABLE outbox (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic      text NOT NULL,
    key        text NOT NULL,
    event_id   text NOT NULL,
    type       text NOT NULL,
    payload    text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
