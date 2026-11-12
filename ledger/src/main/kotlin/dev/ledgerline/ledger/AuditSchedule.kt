package dev.ledgerline.ledger

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

/**
 * Runs the [Audit] on a schedule (`ledger.audit.every`, default 10 minutes; 0: never), so a broken
 * invariant pages someone instead of waiting for the next manual check:
 *
 * - `ledger.audit.violations{check}`: what the last audit found wrong, per check (0 when it holds);
 * - `ledger.audit.last.success`: when an audit last completed (unix seconds), to see it has stopped;
 * - `ledger.audit.duration`: how long it took (it reads the whole event log).
 */
@Component
class AuditSchedule(
    private val audit: Audit,
    meters: MeterRegistry,
    @Value("\${ledger.audit.every:10m}") private val every: Duration,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val last = AtomicReference<Last?>(null)
    @Volatile private var running = false
    private var thread: Thread? = null

    private class Last(val violations: Map<String, Long>, val at: Long, val seconds: Double)

    init {
        for (check in CHECKS) {
            Gauge.builder("ledger.audit.violations", last) { it.get()?.violations?.get(check)?.toDouble() ?: Double.NaN }
                .tag("check", check).register(meters)
        }
        Gauge.builder("ledger.audit.last.success", last) { it.get()?.at?.toDouble() ?: Double.NaN }.baseUnit("seconds").register(meters)
        Gauge.builder("ledger.audit.duration", last) { it.get()?.seconds ?: Double.NaN }.baseUnit("seconds").register(meters)
    }

    override fun start() {
        if (every.isZero) return
        running = true
        thread = Thread.ofPlatform().name("ledger-audit").daemon().start {
            var wait = Duration.ofSeconds(30) // first, once the service has warmed up
            while (running) {
                try {
                    Thread.sleep(wait)
                } catch (_: InterruptedException) {
                    return@start
                }
                wait = every
                try {
                    val started = System.nanoTime()
                    val report = audit.run()
                    last.set(Last(report.violations, System.currentTimeMillis() / 1000, (System.nanoTime() - started) / 1e9))
                    if (!report.ok) log.error("the audit found the books wrong: {}", report.violations.filterValues { it != 0L })
                } catch (e: Exception) {
                    log.warn("audit: {}", e.toString())
                }
            }
        }
    }

    override fun stop() {
        running = false
        thread?.interrupt()
    }

    override fun isRunning() = running

    companion object {
        val CHECKS = listOf(
            "currencies_not_summing_to_zero", "unbalanced_transactions", "balances_not_matching_events",
            "entries_not_matching_events", "transactions_not_matching_events", "overdrawn_accounts",
        )
    }
}
