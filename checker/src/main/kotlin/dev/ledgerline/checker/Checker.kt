package dev.ledgerline.checker

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import tools.jackson.databind.json.JsonMapper
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration

/**
 * Rebuilds the books from Kafka alone and compares them with the services' databases:
 *
 * - from `ledger.events`: every account's balance and version, which must equal the ledger's; and the
 *   number of distinct events, which must equal the ledger's event log;
 * - from `payments.events`: every payment's, refund's and payout's last state and version, which must
 *   equal the payments service's.
 *
 * Like any consumer, it skips an event it has seen (delivery is at least once) and counts a gap in a
 * stream's versions as a failure. A stream whose first event in the topic isn't version 1 (retention
 * deleted the start, or the topic was lost and started again) can't be rebuilt: it is counted apart, as
 * `streams_starting_late`, and not compared. Run it when the outboxes have drained (it says if not).
 *
 *   checker check    (env: KAFKA, LEDGER_DB, LEDGER_DB_USER, LEDGER_DB_PASSWORD, and optionally
 *                     PAYMENTS_DB, PAYMENTS_DB_USER, PAYMENTS_DB_PASSWORD)
 */
fun check(env: Map<String, String>): Pair<Map<String, Any>, Boolean> {
    val kafka = env.getValue("KAFKA")
    val json = JsonMapper.builder().build()
    val report = linkedMapOf<String, Any>()
    var ok = true

    connect(env.getValue("LEDGER_DB"), env.getValue("LEDGER_DB_USER"), env.getValue("LEDGER_DB_PASSWORD")).use { db ->
        val pending = count(db, "SELECT count(*) FROM outbox")
        val stream = rebuild(kafka, "ledger.events", json) { type, data, state ->
            if (type == "Posted") state["balance"] = (state["balance"] as Long? ?: 0L) + data["amount"].asLong()
        }
        val balances = query(db, "SELECT account_id::text, balance, version FROM balances") { "account-${it.getString(1)}" to (it.getLong(2) to it.getInt(3)) }.toMap()
        var wrong = 0
        for ((s, bv) in balances) {
            val rebuilt = stream.streams[s]
            if (rebuilt?.late == true) continue
            if (rebuilt == null || rebuilt.version != bv.second || (rebuilt.state["balance"] as Long? ?: 0L) != bv.first) wrong++
        }
        wrong += stream.streams.keys.count { it.startsWith("account-") && it !in balances }
        val events = count(db, "SELECT count(*) FROM events")
        val r = linkedMapOf(
            "outbox_pending" to pending,
            "records_read" to stream.records,
            "duplicates_skipped" to stream.duplicates,
            "gaps" to stream.gaps,
            "streams_starting_late" to stream.late,
            "distinct_events" to stream.distinct,
            "events_in_ledger" to events,
            "accounts" to balances.size,
            "accounts_not_matching" to wrong,
        )
        report["ledger"] = r
        ok = ok && pending == 0L && stream.gaps == 0 && wrong == 0 && stream.late == 0 && stream.distinct.toLong() == events
    }

    env["PAYMENTS_DB"]?.let { url ->
        connect(url, env.getValue("PAYMENTS_DB_USER"), env.getValue("PAYMENTS_DB_PASSWORD")).use { db ->
            val pending = count(db, "SELECT count(*) FROM outbox")
            val stream = rebuild(kafka, "payments.events", json) { type, _, state -> state["state"] = type.substringAfter('.') }
            val rows = listOf("payment" to "payments", "refund" to "refunds", "payout" to "payouts").flatMap { (entity, table) ->
                query(db, "SELECT id::text, state, version FROM $table") { "$entity-${it.getString(1)}" to (it.getString(2) to it.getInt(3)) }
            }.toMap()
            var wrong = 0
            var missing = 0
            for ((s, sv) in rows) {
                val rebuilt = stream.streams[s]
                when {
                    rebuilt == null -> missing++
                    rebuilt.version != sv.second -> wrong++
                    !rebuilt.late && rebuilt.state["state"] != sv.first -> wrong++
                }
            }
            wrong += stream.streams.keys.count { it !in rows }
            report["payments"] = linkedMapOf(
                "outbox_pending" to pending,
                "records_read" to stream.records,
                "duplicates_skipped" to stream.duplicates,
                "gaps" to stream.gaps,
                "streams" to rows.size,
                "streams_starting_late" to stream.late,
                "streams_not_in_kafka" to missing,
                "streams_not_matching" to wrong,
            )
            ok = ok && pending == 0L && stream.gaps == 0 && wrong == 0 && stream.late == 0 && missing == 0
        }
    }
    report["ok"] = ok
    return report to ok
}

class Rebuilt(var version: Int = 0, val state: MutableMap<String, Any> = mutableMapOf(), var late: Boolean = false)

class Rebuild(val streams: Map<String, Rebuilt>, val records: Long, val duplicates: Long, val gaps: Int, val distinct: Int) {
    val late = streams.count { it.value.late }
}

/** Reads a topic from the start to its end now, applying each stream's events in version order. */
fun rebuild(
    bootstrap: String,
    topic: String,
    json: JsonMapper,
    apply: (type: String, data: tools.jackson.databind.JsonNode, state: MutableMap<String, Any>) -> Unit,
): Rebuild {
    val props = mapOf<String, Any>(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 5_000,
    )
    val streams = HashMap<String, Rebuilt>()
    var records = 0L
    var duplicates = 0L
    var gaps = 0
    KafkaConsumer(props, StringDeserializer(), StringDeserializer()).use { consumer ->
        val partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10)).map { TopicPartition(topic, it.partition()) }
        consumer.assign(partitions)
        consumer.seekToBeginning(partitions)
        val end = consumer.endOffsets(partitions)
        while (partitions.any { consumer.position(it) < end.getValue(it) }) {
            for (rec in consumer.poll(Duration.ofSeconds(1))) {
                records++
                val e = json.readTree(rec.value())
                val s = streams.getOrPut(e["stream"].asString()) { Rebuilt() }
                val v = e["version"].asInt()
                when {
                    s.version == 0 && v > 1 -> {
                        s.late = true
                        s.version = v
                    }
                    v <= s.version -> duplicates++
                    v != s.version + 1 -> {
                        gaps++
                        s.version = v
                    }
                    else -> {
                        s.version = v
                        if (!s.late) apply(e["type"].asString(), e["data"], s.state)
                    }
                }
            }
        }
    }
    return Rebuild(streams, records, duplicates, gaps, streams.values.sumOf { it.version })
}

fun connect(url: String, user: String, password: String): Connection = DriverManager.getConnection(url, user, password)

fun count(db: Connection, sql: String): Long = db.createStatement().use { st -> st.executeQuery(sql).use { it.next(); it.getLong(1) } }

fun <T> query(db: Connection, sql: String, row: (java.sql.ResultSet) -> T): List<T> = db.createStatement().use { st ->
    st.executeQuery(sql).use { rs -> buildList { while (rs.next()) add(row(rs)) } }
}
