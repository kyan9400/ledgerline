-- Events recorded before the outbox existed go through it too, oldest first, so Kafka has the whole
-- history and a consumer can rebuild every account from the start. (On a new database this is empty.)
INSERT INTO outbox (topic, key, event_id, type, payload, created_at)
SELECT 'ledger.events', stream, stream || ':' || version, type,
       jsonb_build_object(
           'id', stream || ':' || version,
           'stream', stream,
           'version', version,
           'type', type,
           'recordedAt', to_char(recorded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
           'data', data)::text,
       recorded_at
FROM events
ORDER BY seq;
