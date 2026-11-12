-- The trace a row's latest change was made in (a W3C traceparent). Whoever acts on the row later (the
-- outbox relay, the worker driving a step, a webhook delivery) continues that trace.
ALTER TABLE outbox ADD COLUMN trace_context text;
ALTER TABLE payments ADD COLUMN trace_context text;
ALTER TABLE refunds ADD COLUMN trace_context text;
ALTER TABLE payouts ADD COLUMN trace_context text;
ALTER TABLE webhook_deliveries ADD COLUMN trace_context text;
