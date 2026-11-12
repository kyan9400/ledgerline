package dev.ledgerline.payments

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.simple.JdbcClient
import java.sql.DriverManager
import java.time.Duration
import java.util.UUID
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * A payment's trace goes where the payment goes, past the call stack: into its row (which the worker
 * continues from), into its events' Kafka messages (through the outbox), and over HTTP into the ledger,
 * whose own change and outbox row are in the same trace.
 */
class TracingTest : PaymentsTest() {
    @Autowired
    lateinit var db: JdbcClient

    @Test
    fun `a payment's trace runs through its row, its Kafka messages and the ledger`() {
        val traceId = (1..32).map { "0123456789abcdef"[Random.nextInt(16)] }.joinToString("")
        val alice = open("alice", funded = 10_000)
        val shop = open("shop", "merchant")
        val auth = call(
            "POST", "/v1/payments", mapOf("customer" to alice, "merchant" to shop, "amount" to 2_500, "description" to "traced"),
            headers = mapOf("traceparent" to "00-$traceId-00f067aa0ba902b7-01"),
        )
        assertEquals(201, auth.status, auth.raw)
        val id = UUID.fromString(auth.body["id"].asString())

        val row = db.sql("SELECT trace_context FROM payments WHERE id = :id").param("id", id).query(String::class.java).single()
        assertTrue(row.startsWith("00-$traceId-"), "the row keeps the trace for whoever drives it next: $row")

        // Each of its events reaches Kafka with a traceparent in the same trace (a send span of its own).
        val sent = messageTraces(listOf("payment-$id:1", "payment-$id:2"))
        assertEquals(2, sent.size, "both events were sent: $sent")
        sent.forEach { (event, tp) -> assertTrue(tp.startsWith("00-$traceId-") && tp != row, "$event: $tp") }

        // The ledger was called in the trace: its transaction's outbox row (its relay is off in the tests) is in it.
        val ledgerTraces = DriverManager.getConnection(World.ledgerDbUrl(), World.user(), World.password()).use { c ->
            c.prepareStatement("SELECT trace_context FROM outbox WHERE payload LIKE ?").use { s ->
                s.setString(1, "%$id%")
                s.executeQuery().use { rs -> generateSequence { if (rs.next()) rs.getString(1) else null }.toList() }
            }
        }
        assertTrue(ledgerTraces.isNotEmpty(), "the ledger recorded the authorisation")
        ledgerTraces.forEach { assertTrue(it.startsWith("00-$traceId-"), "the ledger's change is in the trace: $it") }
    }

    /** The traceparent header of each event's Kafka message, by event id. */
    private fun messageTraces(events: List<String>): Map<String, String> {
        val bootstrap = System.getenv("LEDGERLINE_TEST_KAFKA").orEmpty()
        assumeTrue(bootstrap.isNotBlank(), "needs Kafka (LEDGERLINE_TEST_KAFKA)")
        val found = HashMap<String, String>()
        KafkaConsumer(
            mapOf<String, Any>(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false),
            StringDeserializer(), StringDeserializer(),
        ).use { consumer ->
            val parts = consumer.partitionsFor(PAYMENTS_TOPIC).map { TopicPartition(it.topic(), it.partition()) }
            consumer.assign(parts)
            consumer.seekToBeginning(parts)
            eventually(Duration.ofSeconds(30), "the events on $PAYMENTS_TOPIC") {
                for (r in consumer.poll(Duration.ofMillis(500))) {
                    val event = r.headers().lastHeader("event-id")?.value()?.decodeToString() ?: continue
                    if (event in events) found[event] = r.headers().lastHeader("traceparent")?.value()?.decodeToString() ?: "none"
                }
                found.keys.containsAll(events)
            }
        }
        return found
    }
}
