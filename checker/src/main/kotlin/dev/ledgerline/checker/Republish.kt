package dev.ledgerline.checker

/**
 * Fills Kafka again from a service's event log, after Kafka has lost its data: the outbox is replaced by
 * every event, oldest first, and the service's relay sends them.
 *
 * Writes pause while it runs: it takes the event log in EXCLUSIVE mode (a writer appends to the event log
 * and the outbox in one transaction, so none is half-way through) and the relay's lock (no batch is in
 * flight). Run it with the topic empty and before the services send anything new, that is with the
 * services stopped or just started: otherwise a stream's newest events could reach the new topic before
 * its oldest. Consumers skip what they have already seen, so a consumer that kept its state is not
 * disturbed.
 *
 *   checker republish ledger      (env: LEDGER_DB, LEDGER_DB_USER, LEDGER_DB_PASSWORD)
 *   checker republish payments    (env: PAYMENTS_DB, PAYMENTS_DB_USER, PAYMENTS_DB_PASSWORD)
 */
fun republish(env: Map<String, String>, service: String): Pair<Map<String, Any>, Boolean> {
    val (prefix, topic) = when (service) {
        "ledger" -> "LEDGER" to "ledger.events"
        "payments" -> "PAYMENTS" to "payments.events"
        else -> error("republish ledger | payments")
    }
    connect(env.getValue("${prefix}_DB"), env.getValue("${prefix}_DB_USER"), env.getValue("${prefix}_DB_PASSWORD")).use { db ->
        db.autoCommit = false
        val started = System.nanoTime()
        db.createStatement().use { st ->
            st.execute("SELECT pg_advisory_xact_lock($RELAY_LOCK)")
            st.execute("LOCK TABLE events IN EXCLUSIVE MODE")
            val dropped = st.executeUpdate("DELETE FROM outbox")
            val queued = st.executeUpdate(
                """INSERT INTO outbox (topic, key, event_id, type, payload, created_at)
                   SELECT '$topic', stream, stream || ':' || version, type,
                          jsonb_build_object('id', stream || ':' || version, 'stream', stream, 'version', version, 'type', type,
                                             'recordedAt', to_char(recorded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                                             'data', data)::text,
                          recorded_at
                   FROM events ORDER BY seq""",
            )
            db.commit()
            return linkedMapOf(
                "service" to service,
                "topic" to topic,
                "outbox_rows_replaced" to dropped,
                "events_queued" to queued,
                "seconds" to (System.nanoTime() - started) / 1e9,
            ) to true
        }
    }
}

/** The relay's advisory lock (dev.ledgerline.platform.OutboxRelay). */
private const val RELAY_LOCK = 0x6f7574626f78L
