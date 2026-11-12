-- The trace an outbox event's change was made in (a W3C traceparent), so its Kafka message continues it.
ALTER TABLE outbox ADD COLUMN trace_context text;
