package dev.ledgerline.payments

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.Test
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The payments service asking a (stub) fraud scorer, in enforce mode. */
class FraudCheckTest : PaymentsTest() {
    companion object {
        /** Declines 66,600; answers 77,700 too late (after a second); approves the rest. */
        val asked = ConcurrentHashMap<String, AtomicInteger>()
        private val stub: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 50).apply {
            executor = Executors.newVirtualThreadPerTaskExecutor()
            createContext("/v1/score") { ex ->
                val body = ex.requestBody.readAllBytes().decodeToString()
                Regex("\"payment\":\"([^\"]+)\"").find(body)?.let { asked.computeIfAbsent(it.groupValues[1]) { AtomicInteger() }.incrementAndGet() }
                val reply = when {
                    "\"amount\":66600" in body -> """{"score":0.97,"decision":"decline","model":"stub","reasons":["n_1m","small_10m"]}"""
                    "\"amount\":77700" in body -> { Thread.sleep(1_000); """{"score":0.01,"decision":"approve","model":"stub","reasons":[]}""" }
                    else -> """{"score":0.02,"decision":"approve","model":"stub","reasons":[]}"""
                }
                ex.sendResponseHeaders(200, reply.length.toLong())
                ex.responseBody.use { it.write(reply.toByteArray()) }
            }
            start()
        }

        @JvmStatic
        @DynamicPropertySource
        fun fraud(registry: DynamicPropertyRegistry) {
            registry.add("payments.fraud.url") { "http://127.0.0.1:${stub.address.port}" }
            registry.add("payments.fraud.mode") { "enforce" }
        }
    }

    private fun authorise(customer: UUID, merchant: UUID, amount: Long) =
        call("POST", "/v1/payments", mapOf("customer" to customer, "merchant" to merchant, "amount" to amount))

    @Test
    fun `a payment the scorer declines is declined before any money moves`() {
        val ivy = open("ivy", funded = 100_000)
        val shop = open("shop", "merchant")
        val p = authorise(ivy, shop, 66_600)
        assertEquals("declined", p.body["state"].asString(), p.raw)
        assertEquals("fraud_suspected", p.body["declineReason"].asString())
        assertEquals("decline", p.body["fraudDecision"].asString())
        assertEquals(0.97, p.body["fraudScore"].asDouble())
        assertEquals("n_1m,small_10m", p.body["fraudReasons"].asString())
        assertEquals(100_000, balance(ivy), "nothing was held")
    }

    @Test
    fun `an approved payment goes on, with its score kept, and the scorer is asked once`() {
        val jo = open("jo", funded = 100_000)
        val shop = open("shop", "merchant")
        val p = authorise(jo, shop, 12_345)
        assertEquals("authorised", p.body["state"].asString(), p.raw)
        assertEquals("approve", p.body["fraudDecision"].asString())
        assertEquals(0.02, p.body["fraudScore"].asDouble())
        assertEquals(87_655, balance(jo))
        assertEquals(1, asked.getValue(p.body["id"].asString()).get())
    }

    @Test
    fun `a scorer that doesn't answer in time doesn't stop the payment, which goes on unscored`() {
        val kim = open("kim", funded = 100_000)
        val shop = open("shop", "merchant")
        val started = System.nanoTime()
        val p = authorise(kim, shop, 77_700)
        val ms = (System.nanoTime() - started) / 1_000_000
        assertEquals("authorised", p.body["state"].asString(), p.raw)
        assertEquals("unscored", p.body["fraudDecision"].asString())
        assertNull(p.body["fraudScore"]?.takeIf { !it.isNull })
        assertTrue(ms < 900, "it waited the scorer's timeout (150 ms), not the scorer: $ms ms")
    }
}
