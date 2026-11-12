package dev.ledgerline.platform

import org.springframework.jdbc.core.simple.JdbcClient
import tools.jackson.databind.json.JsonMapper
import java.time.Instant

/**
 * The transactional outbox: an event is written to the `outbox` table in the same database transaction
 * as the change it describes, so it is published if and only if the change commits. [OutboxRelay] then
 * sends it to Kafka.
 *
 * Each event belongs to a stream (an account, a payment) and has a version there, from 1. Its id is
 * `stream:version`. A consumer may see an event more than once (the relay publishes at least once), so it
 * keeps, per stream, the last version it applied and skips the rest: per stream, versions only grow.
 *
 * Each service's migrations create the table:
 * ```
 * CREATE TABLE outbox (
 *     id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 *     topic      text NOT NULL,
 *     key        text NOT NULL,
 *     event_id   text NOT NULL,
 *     type       text NOT NULL,
 *     payload    text NOT NULL,
 *     created_at timestamptz NOT NULL DEFAULT now(),
 *     trace_context text  -- the trace the change was made in (see [Traces])
 * );
 * ```
 */
class Outbox(private val db: JdbcClient, private val json: JsonMapper, private val traces: Traces = Traces.NONE) {

    /** Within the caller's transaction. The Kafka key is the stream, so a stream's events stay in order. */
    fun add(topic: String, stream: String, version: Int, type: String, data: Any?, at: Instant) =
        addJson(topic, stream, version, type, json.writeValueAsString(data), at)

    /** The same, with the data already as JSON (the ledger has just written it to its event log). */
    fun addJson(topic: String, stream: String, version: Int, type: String, dataJson: String, at: Instant) {
        val id = "$stream:$version"
        val payload = buildString(dataJson.length + 200) {
            append("{\"id\":").append(json.writeValueAsString(id))
            append(",\"stream\":").append(json.writeValueAsString(stream))
            append(",\"version\":").append(version)
            append(",\"type\":").append(json.writeValueAsString(type))
            append(",\"recordedAt\":\"").append(at).append('"')
            append(",\"data\":").append(dataJson).append('}')
        }
        db.sql("INSERT INTO outbox (topic, key, event_id, type, payload, trace_context) VALUES (:topic, :key, :id, :type, :payload, :tc)")
            .param("topic", topic).param("key", stream).param("id", id).param("type", type).param("payload", payload)
            .param("tc", traces.current()).update()
    }
}
