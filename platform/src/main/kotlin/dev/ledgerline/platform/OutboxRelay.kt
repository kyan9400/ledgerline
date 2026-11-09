package dev.ledgerline.platform

import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

data class RelaySettings(
    val bootstrapServers: String,
    val topics: List<String>,
    val partitions: Int = 6,
    val batch: Int = 500,
    val pollMs: Long = 50,
)

/**
 * Sends the outbox to Kafka, oldest first, and deletes what Kafka has acknowledged.
 *
 * One relay works at a time per database (a transaction-scoped advisory lock), so a service can run
 * several instances. A batch is read, sent with an idempotent producer (acks=all) and deleted, in one
 * database transaction. If the relay dies after sending and before the delete commits, the batch is sent
 * again: delivery is at least once, and consumers skip what they have seen (see [Outbox]).
 *
 * Order: two changes to one stream lock the same row, so the second's outbox row is written (and gets its
 * id) only after the first has committed. Reading in id order therefore keeps each stream in order, and
 * the stream is the Kafka key, so it stays in order in its partition. A row not yet committed is
 * invisible, and a later batch picks it up.
 *
 * If Kafka is down the services keep working: the outbox grows, and drains when Kafka is back
 * (`outbox.lag.seconds` says how far behind it is).
 */
class OutboxRelay(
    private val db: JdbcClient,
    private val tx: TransactionTemplate,
    private val settings: RelaySettings,
    meters: MeterRegistry,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val lagMs = AtomicLong(0)
    private val published = meters.counter("outbox.published")
    private val errors = meters.counter("outbox.relay.errors")
    @Volatile private var running = false
    private var thread: Thread? = null
    private var producer: KafkaProducer<String, String>? = null

    init {
        meters.gauge("outbox.lag.seconds", lagMs) { it.get() / 1000.0 }
    }

    override fun start() {
        if (settings.bootstrapServers.isBlank()) {
            log.info("outbox relay off: no Kafka bootstrap servers (outbox.kafka.bootstrap-servers)")
            return
        }
        producer = KafkaProducer(
            mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to settings.bootstrapServers,
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
                ProducerConfig.LINGER_MS_CONFIG to 5,
                ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
                ProducerConfig.MAX_BLOCK_MS_CONFIG to 5_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG to 10_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to 30_000,
            ),
            StringSerializer(), StringSerializer(),
        )
        running = true
        thread = Thread.ofPlatform().name("outbox-relay").daemon().start(::loop)
    }

    override fun stop() {
        running = false
        thread?.interrupt()
        thread?.join(10_000)
        producer?.close(Duration.ofSeconds(5))
    }

    override fun isRunning() = running

    private fun loop() {
        var topicsReady = false
        var failures = 0
        while (running) {
            try {
                if (!topicsReady) {
                    createTopics()
                    topicsReady = true
                }
                val sent = relayOnce()
                failures = 0
                if (sent < settings.batch) Thread.sleep(settings.pollMs)
            } catch (_: InterruptedException) {
                return
            } catch (e: Exception) {
                if (!running) return
                errors.increment()
                failures++
                topicsReady = false // a topic may have gone (a broker lost its data): make sure of them again
                if (failures == 1 || failures % 20 == 0) log.warn("outbox relay: {} (failure {})", e.toString(), failures)
                try {
                    Thread.sleep((100L shl minOf(failures, 6)).coerceAtMost(5_000))
                } catch (_: InterruptedException) {
                    return
                }
            }
        }
    }

    /** One batch: returns how many events were sent. */
    fun relayOnce(): Int = tx.execute {
        val mine = db.sql("SELECT pg_try_advisory_xact_lock(:k)").param("k", LOCK).query(Boolean::class.java).single()
        if (!mine) return@execute 0
        val rows = db.sql("SELECT id, topic, key, event_id, type, payload, created_at FROM outbox ORDER BY id LIMIT :n")
            .param("n", settings.batch)
            .query { rs, _ ->
                Row(
                    rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6),
                    rs.getTimestamp(7).toInstant(),
                )
            }.list()
        lagMs.set(rows.firstOrNull()?.let { Duration.between(it.createdAt, Instant.now()).toMillis().coerceAtLeast(0) } ?: 0)
        if (rows.isEmpty()) return@execute 0
        val p = producer!!
        val acks = rows.map { r ->
            val record = ProducerRecord(r.topic, r.key, r.payload)
            record.headers().add("event-id", r.eventId.toByteArray()).add("type", r.type.toByteArray())
            p.send(record)
        }
        p.flush()
        acks.forEach { it.get(30, TimeUnit.SECONDS) } // a failure rolls back: the batch stays, and is sent again
        // Exactly the rows sent: a range (id <= the last) could take a row that committed after the read.
        db.sql("DELETE FROM outbox WHERE id = ANY(:ids)").param("ids", rows.map { it.id }.toTypedArray()).update()
        published.increment(rows.size.toDouble())
        rows.size
    }

    private fun createTopics() {
        Admin.create(mapOf<String, Any>(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to settings.bootstrapServers)).use { admin ->
            for (topic in settings.topics) {
                try {
                    admin.createTopics(listOf(NewTopic(topic, settings.partitions, 1.toShort()))).all().get(10, TimeUnit.SECONDS)
                    log.info("created topic {} ({} partitions)", topic, settings.partitions)
                } catch (e: ExecutionException) {
                    if (e.cause !is TopicExistsException) throw e
                }
            }
        }
    }

    private class Row(
        val id: Long,
        val topic: String,
        val key: String,
        val eventId: String,
        val type: String,
        val payload: String,
        val createdAt: Instant,
    )

    companion object {
        private const val LOCK = 0x6f7574626f78L // "outbox"
    }
}
