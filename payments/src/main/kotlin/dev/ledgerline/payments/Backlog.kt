package dev.ledgerline.payments

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.jdbc.core.simple.JdbcClient
import java.util.concurrent.atomic.AtomicReference

/**
 * How far behind the work is, for the alerts: per kind of row, how many wait for their next step
 * (`payments.backlog.rows`) and how long the oldest has waited in its state (`payments.backlog.oldest`).
 *
 * A payment held up by a ledger that is down still answers its client (202: the worker will finish it),
 * so the request metrics look fine; this is what shows it. Measured every few seconds by a thread of its
 * own, from the partial indexes on `next_attempt_at` (only waiting rows are in them), not per scrape.
 */
class Backlog(private val db: JdbcClient, meters: MeterRegistry, private val everyMs: Long = 5_000) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val now = AtomicReference(TABLES.keys.associateWith { Measured(0, 0.0) })
    @Volatile private var running = false
    private var thread: Thread? = null

    private data class Measured(val rows: Long, val oldestSeconds: Double)

    init {
        for (entity in TABLES.keys) {
            Gauge.builder("payments.backlog.rows", now) { it.get().getValue(entity).rows.toDouble() }
                .tag("entity", entity).description("rows waiting for their next step").register(meters)
            Gauge.builder("payments.backlog.oldest", now) { it.get().getValue(entity).oldestSeconds }
                .tag("entity", entity).baseUnit("seconds").description("how long the oldest waiting row has been in its state")
                .register(meters)
        }
    }

    fun measure() {
        now.set(
            TABLES.mapValues { (_, t) ->
                db.sql(
                    """SELECT count(*), coalesce(extract(epoch FROM now() - min(${t.since})), 0)::float8
                       FROM ${t.table} WHERE next_attempt_at IS NOT NULL""",
                ).query { rs, _ -> Measured(rs.getLong(1), rs.getDouble(2)) }.single()
            },
        )
    }

    override fun start() {
        running = true
        thread = Thread.ofPlatform().name("payments-backlog").daemon().start {
            while (running) {
                try {
                    measure()
                } catch (e: Exception) {
                    log.debug("backlog: {}", e.toString()) // the database is down: the last values stand
                }
                try {
                    Thread.sleep(everyMs)
                } catch (_: InterruptedException) {
                    return@start
                }
            }
        }
    }

    override fun stop() {
        running = false
        thread?.interrupt()
    }

    override fun isRunning() = running

    private class Table(val table: String, val since: String)

    companion object {
        private val TABLES = mapOf(
            "payment" to Table("payments", "updated_at"),
            "refund" to Table("refunds", "updated_at"),
            "payout" to Table("payouts", "updated_at"),
            "webhook" to Table("webhook_deliveries", "created_at"),
        )
    }
}
