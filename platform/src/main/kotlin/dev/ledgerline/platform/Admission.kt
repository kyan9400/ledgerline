package dev.ledgerline.platform

import io.micrometer.core.instrument.MeterRegistry
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.web.filter.OncePerRequestFilter
import tools.jackson.databind.json.JsonMapper
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

/**
 * Load shedding: at most `admission.max-in-flight` API requests (under /v1/) at once; the next waits up
 * to `admission.wait-ms` for a place, then gets 503 with Retry-After.
 *
 * With virtual threads every request gets a thread at once, so without this an overload doesn't queue
 * in Tomcat: it piles up inside the service, waiting for database connections, until the heap runs out.
 * (It did: at 1,000 transfers a second, 1,500 requests were waiting and the ledger's JVM died.) A fast
 * refusal is safe to retry, since every write carries an idempotency key.
 */
class Admission(
    maxInFlight: Int,
    private val waitMs: Long,
    private val json: JsonMapper,
    meters: MeterRegistry,
) : OncePerRequestFilter() {
    private val permits = Semaphore(maxInFlight, true)
    private val shed = meters.counter("admission.shed")

    init {
        meters.gauge("admission.in_flight", permits) { maxInFlight - it.availablePermits().toDouble() }
    }

    override fun shouldNotFilter(request: HttpServletRequest) = !request.requestURI.startsWith("/v1/")

    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain) {
        if (!permits.tryAcquire(waitMs, TimeUnit.MILLISECONDS)) {
            shed.increment()
            response.status = 503
            response.contentType = "application/problem+json"
            response.setHeader("Retry-After", "1")
            response.writer.write(json.writeValueAsString(BUSY))
            return
        }
        try {
            chain.doFilter(request, response)
        } finally {
            permits.release()
        }
    }

    companion object {
        /** RFC 9457 problem details, as the services' other refusals. */
        val BUSY = linkedMapOf(
            "type" to "https://github.com/kyan9400/ledgerline#busy",
            "title" to "Busy",
            "status" to 503,
            "code" to "busy",
            "detail" to "too many requests at once: retry in a second with the same Idempotency-Key",
        )
    }
}
