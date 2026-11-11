-- Merchants: who may use the gRPC API, as which ledger account, and where their webhooks go.
CREATE TABLE merchants (
    id             uuid PRIMARY KEY,
    name           text NOT NULL,
    account        uuid NOT NULL UNIQUE,     -- their merchant account in the ledger
    api_key_hash   text NOT NULL UNIQUE,     -- sha256 of the API key; the key itself is shown once
    webhook_url    text,                     -- none: no webhooks
    -- The signing secret is needed to sign, so it is kept (in production: encrypted, or in a vault).
    webhook_secret text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- Webhooks to deliver: queued in the same transaction as the change they describe (an outbox again),
-- sent by the worker, retried with backoff, and kept as failed after the last try (they can be
-- redelivered). One per event and merchant.
CREATE TABLE webhook_deliveries (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_id     uuid NOT NULL REFERENCES merchants (id),
    event_id        text NOT NULL,           -- stream:version, as in Kafka
    type            text NOT NULL,
    payload         text NOT NULL,           -- the exact bytes signed and sent
    state           text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'delivered', 'failed')),
    attempts        integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    delivered_at    timestamptz,
    UNIQUE (merchant_id, event_id)
);
CREATE INDEX webhook_deliveries_due ON webhook_deliveries (next_attempt_at) WHERE next_attempt_at IS NOT NULL;
