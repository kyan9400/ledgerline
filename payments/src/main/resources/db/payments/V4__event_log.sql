-- The payments service's own event log: every change of state of a payment, refund or payout, in order,
-- only ever appended to (as the ledger's). The outbox is a relay, emptied as it is sent; this is the
-- history, from which Kafka can be filled again if it ever loses its data (checker republish payments).
CREATE TABLE events (
    seq         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream      text NOT NULL,      -- payment-<id>, refund-<id>, payout-<id>
    version     integer NOT NULL CHECK (version >= 1),
    type        text NOT NULL,      -- payment.authorised, payout.settled, ...
    data        jsonb NOT NULL,     -- the row as it was after the change
    recorded_at timestamptz NOT NULL,
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
