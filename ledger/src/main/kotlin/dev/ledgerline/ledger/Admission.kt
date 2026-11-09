package dev.ledgerline.ledger

import io.micrometer.core.instrument.MeterRegistry
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import tools.jackson.databind.json.JsonMapper
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

/**
 * Load shedding: at most `ledger.max-in-flight` API requests at once; the next waits up to
 * `ledger.admission-wait-ms` for a place, then gets 503 with Retry-After.
 *
 * With virtual threads every request gets a thread at once, so without this an overload doesn't queue
 * in Tomcat: it piles up inside the service, waiting for database connections, until the heap runs out.
 * (It did: at 1,000 transfers a second, 1,500 requests were waiting and the JVM died.) A fast refusal is
 * safe to retry, since every write carries an idempotency key.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class Admission(
    @Value("\${ledger.max-in-flight:32}") maxInFlight: Int,
    @Value("\${ledger.admission-wait-ms:200}") private val waitMs: Long,
    private val json: JsonMapper,
    meters: MeterRegistry,
) : OncePerRequestFilter() {
    private val permits = Semaphore(maxInFlight, true)
    private val shed = meters.counter("ledger.requests.shed")

    init {
        meters.gauge("ledger.requests.in_flight", permits) { maxInFlight - it.availablePermits().toDouble() }
    }

    override fun shouldNotFilter(request: HttpServletRequest) = !request.requestURI.startsWith("/v1/")

    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain) {
        if (!permits.tryAcquire(waitMs, TimeUnit.MILLISECONDS)) {
            shed.increment()
            response.status = 503
            response.contentType = "application/problem+json"
            response.setHeader("Retry-After", "1")
            response.writer.write(json.writeValueAsString(problem(Busy())))
            return
        }
        try {
            chain.doFilter(request, response)
        } finally {
            permits.release()
        }
    }
}

class Busy : LedgerException(503, "busy", "too many requests at once: retry in a second with the same Idempotency-Key")
