package dev.ledgerline.ledger

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import jakarta.servlet.FilterChain
import org.junit.jupiter.api.Test
import org.springframework.mock.web.MockFilterChain
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import tools.jackson.databind.json.JsonMapper
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AdmissionTest {
    private val meters = SimpleMeterRegistry()
    private val admission = Admission(maxInFlight = 1, waitMs = 50, json = JsonMapper.builder().build(), meters = meters)

    private fun call(uri: String, chain: FilterChain = MockFilterChain()) =
        MockHttpServletResponse().also { admission.doFilter(MockHttpServletRequest("POST", uri), it, chain) }

    @Test
    fun `over the limit a request is refused at once with 503, and the slot is free again after`() {
        val inside = CountDownLatch(1)
        val release = CountDownLatch(1)
        val busy = thread { call("/v1/transfers", FilterChain { _, _ -> inside.countDown(); release.await() }) }
        inside.await()

        val started = System.nanoTime()
        val refused = call("/v1/transfers")
        assertEquals(503, refused.status)
        assertEquals("1", refused.getHeader("Retry-After"))
        assertTrue(refused.contentAsString.contains("\"code\":\"busy\""))
        assertTrue((System.nanoTime() - started) / 1_000_000 < 1_000, "refused after a short wait, not queued")
        assertEquals(200, call("/actuator/health").status) // health and internal checks are never shed
        assertEquals(1.0, meters.counter("ledger.requests.shed").count())

        release.countDown()
        busy.join()
        assertEquals(200, call("/v1/transfers").status)
    }
}
