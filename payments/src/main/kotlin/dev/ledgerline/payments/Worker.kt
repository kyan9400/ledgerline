package dev.ledgerline.payments

import dev.ledgerline.platform.Traces
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.tracing.Span
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

/**
 * Drives every step that is due: one the API couldn't finish, a retry after a backoff, the next step
 * of a payout, a hold that has expired. Each due row is leased first (Machine.claimDue), so several
 * instances of the service share the work without taking the same row; a lease that runs out (the
 * instance died) makes the row due again.
 *
 * Each step runs in a span continuing the trace of the row's last change: a payment the worker finishes
 * after a crash shows in the trace of the request that started it.
 */
class Worker(
    private val payments: Payments,
    private val payouts: Payouts,
    private val webhooks: Webhooks?,
    private val pollMs: Long,
    private val parallelism: Int,
    meters: MeterRegistry,
    private val traces: Traces = Traces.NONE,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val executor = Executors.newVirtualThreadPerTaskExecutor()
    private val slots = Semaphore(parallelism)
    private val driving = ConcurrentHashMap.newKeySet<Any>()
    private val failures = meters.counter("payments.worker.errors")
    @Volatile private var running = false
    private var thread: Thread? = null

    override fun start() {
        running = true
        thread = Thread.ofPlatform().name("payments-worker").daemon().start(::loop)
    }

    override fun stop() {
        running = false
        thread?.interrupt()
        thread?.join(10_000)
        executor.shutdown()
    }

    override fun isRunning() = running

    private fun loop() {
        while (running) {
            try {
                val found = round()
                if (found == 0) Thread.sleep(pollMs)
            } catch (_: InterruptedException) {
                return
            } catch (e: Exception) {
                if (!running) return
                failures.increment()
                log.warn("worker: {}", e.toString())
                try {
                    Thread.sleep(1_000)
                } catch (_: InterruptedException) {
                    return
                }
            }
        }
    }

    /** One pass: returns how many rows it took on. */
    fun round(): Int {
        payments.expire(parallelism)
        var found = 0
        found += dispatch("payment step", null, payments.payments.claimDue(parallelism)) { payments.drivePayment(it) }
        found += dispatch("refund step", null, payments.refunds.claimDue(parallelism)) { payments.driveRefund(it) }
        found += dispatch("payout step", null, payouts.payouts.claimDue(parallelism)) { payouts.drive(it) }
        if (webhooks != null) found += dispatch("webhook delivery", Span.Kind.CLIENT, webhooks.claimDue(parallelism)) { webhooks.deliver(it) }
        return found
    }

    private fun <K : Any> dispatch(what: String, kind: Span.Kind?, due: List<Due<K>>, drive: (K) -> Unit): Int {
        for ((id, trace) in due) {
            if (!driving.add(id)) continue
            slots.acquire()
            executor.execute {
                try {
                    traces.within(trace, what, kind, mapOf("ledgerline.id" to id.toString())) { drive(id) }
                } catch (e: Exception) {
                    // A step that threw (the ledger was unreachable while opening a system account, say)
                    // stays leased, and is due again when the lease runs out.
                    failures.increment()
                    log.debug("drive {}: {}", id, e.toString())
                } finally {
                    driving.remove(id)
                    slots.release()
                }
            }
        }
        return due.size
    }
}
