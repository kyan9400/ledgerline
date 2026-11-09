-- The transactional outbox (dev.ledgerline.platform.Outbox): every event is also written here, in the
-- same transaction, and the relay sends it to Kafka and deletes it.
CREATE TABLE outbox (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic      text NOT NULL,
    key        text NOT NULL,
    event_id   text NOT NULL,
    type       text NOT NULL,
    payload    text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
