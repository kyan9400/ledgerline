package dev.ledgerline.platform

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.context.SmartLifecycle
import org.springframework.jdbc.core.simple.JdbcClient
import java.util.concurrent.atomic.AtomicInteger

/**
 * Whether the service can reach its database, every few seconds: `ledgerline.database.up` (1 or 0).
 *
 * The request metrics can't say it: a service whose database is gone still answers its health scrape,
 * and a two-minute outage spends too little of the month's error budget to page on its own (the alert
 * evaluation showed both). This is the cause the symptoms point to.
 */
class DatabaseProbe(private val db: JdbcClient, meters: MeterRegistry, private val everyMs: Long = 5_000) : SmartLifecycle {
    private val up = AtomicInteger(1)
    @Volatile private var running = false
    private var thread: Thread? = null

    init {
        Gauge.builder("ledgerline.database.up", up) { it.get().toDouble() }
            .description("1 when the last query to the service's database succeeded").register(meters)
    }

    fun probe() {
        up.set(
            try {
                db.sql("SELECT 1").query(Int::class.java).single()
                1
            } catch (_: Exception) {
                0
            },
        )
    }

    override fun start() {
        running = true
        thread = Thread.ofPlatform().name("database-probe").daemon().start {
            while (running) {
                probe()
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
}
