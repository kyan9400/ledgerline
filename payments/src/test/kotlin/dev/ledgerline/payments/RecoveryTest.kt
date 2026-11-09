package dev.ledgerline.payments

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.simple.JdbcClient
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The payments service talks to a ledger whose answers get lost: of every call, a fifth fails before
 * reaching the ledger, and a fifth reaches it, takes effect, and then the answer is lost on the way back
 * (the worst case: the money moved, and the caller doesn't know). Random payments, captures, voids,
 * refunds and payouts run against it at once. When it all settles, every balance must be exactly what
 * the final states say, the ledger's audit must pass, and the reconciliation must hold.
 */
class RecoveryTest : PaymentsTest() {

    class Flaky(private val real: LedgerClient, private val random: Random) : LedgerClient {
        val lostBefore = AtomicInteger()
        val lostAfter = AtomicInteger()

        override fun post(path: String, idempotencyKey: String, body: Any): Outcome = flaky { real.post(path, idempotencyKey, body) }

        override fun get(path: String): Outcome = flaky { real.get(path) }

        private fun flaky(call: () -> Outcome): Outcome {
            val roll = synchronized(random) { random.nextDouble() }
            if (roll < 0.2) return Outcome.Unavailable("lost before").also { lostBefore.incrementAndGet() }
            val outcome = call()
            if (roll < 0.4) return Outcome.Unavailable("lost after").also { lostAfter.incrementAndGet() }
            return outcome
        }
    }

    @TestConfiguration
    class FlakyLedger {
        @Bean
        @Primary
        fun flakyLedgerClient(@Value("\${payments.ledger.url}") url: String, json: JsonMapper) =
            Flaky(HttpService(url, Duration.ofSeconds(5), json), Random(20261109))
    }

    @Autowired
    lateinit var flaky: LedgerClient

    @Autowired
    lateinit var db: JdbcClient

    /** A client that does what clients should: retries a 503 with the same key. */
    private fun send(method: String, path: String, body: Any? = null): Reply {
        val key = UUID.randomUUID().toString()
        repeat(50) {
            val r = call(method, path, body, key)
            if (r.status != 503) return r
            Thread.sleep(20)
        }
        error("$method $path: still 503 after 50 tries")
    }

    @Test
    fun `with lost ledger answers, every payment, refund and payout settles to exactly the right balances`() {
        val customers = (1..8).map { open("customer$it", funded = 500_000) }
        val merchants = (1..3).map { open("merchant$it", "merchant", funded = 50_000) }
        val payments = ConcurrentHashMap.newKeySet<String>()
        val payouts = ConcurrentHashMap.newKeySet<String>()
        val pool = Executors.newFixedThreadPool(16)
        val random = Random(7)
        repeat(240) { i ->
            val customer = customers[random.nextInt(customers.size)]
            val merchant = merchants[random.nextInt(merchants.size)]
            val amount = 100L + random.nextLong(60_000)
            val then = random.nextInt(4) // 0: leave it, 1: capture all, 2: capture part, 3: void
            val refunds = random.nextInt(3)
            val isPayout = i % 6 == 0
            pool.execute {
                if (isPayout) {
                    val r = send("POST", "/v1/payouts", mapOf("account" to customer, "amount" to amount, "bank" to "otherbank", "beneficiary" to "ru-$i"))
                    if (r.status == 202) payouts += r.body["id"].asString()
                    return@execute
                }
                val auth = send("POST", "/v1/payments", mapOf("customer" to customer, "merchant" to merchant, "amount" to amount))
                if (auth.status !in setOf(201, 202)) return@execute
                val id = auth.body["id"].asString()
                payments += id
                // An answer of 202 means the worker is finishing it: wait, as a client would.
                eventually(Duration.ofSeconds(60), "payment $id authorised or declined") { get("/v1/payments/$id").body["state"].asString() != "authorising" }
                if (get("/v1/payments/$id").body["state"].asString() != "authorised") return@execute
                when (then) {
                    1 -> send("POST", "/v1/payments/$id/capture")
                    2 -> send("POST", "/v1/payments/$id/capture", mapOf("amount" to amount / 2 + 1))
                    3 -> send("POST", "/v1/payments/$id/void")
                }
                if (then in 1..2) {
                    eventually(Duration.ofSeconds(60), "payment $id captured") { get("/v1/payments/$id").body["state"].asString() != "capturing" }
                    repeat(refunds) { send("POST", "/v1/payments/$id/refunds", mapOf("amount" to amount / 4)) }
                }
            }
        }
        pool.shutdown()
        assertTrue(pool.awaitTermination(5, TimeUnit.MINUTES))
        Thread.sleep(6_000) // the holds left alone expire (5 s in the tests), so no state changes after the check
        reconciled(Duration.ofSeconds(120))
        val audit = audit()
        assertTrue(audit["ok"].asBoolean(), "ledger audit: $audit")

        // The balances the final states imply, from the payments service's own records.
        val expected = mutableMapOf<UUID, Long>()
        customers.forEach { expected[it] = 500_000 }
        merchants.forEach { expected[it] = 50_000 }
        val states = mutableMapOf<String, Int>()
        for (id in payments) {
            val p = get("/v1/payments/$id").body
            val customer = UUID.fromString(p["customer"].asString())
            val merchant = UUID.fromString(p["merchant"].asString())
            states.merge("payment:" + p["state"].asString(), 1, Int::plus)
            when (p["state"].asString()) {
                "authorised" -> expected.merge(customer, -p["amount"].asLong(), Long::plus)
                "captured" -> {
                    expected.merge(customer, -p["captured"].asLong(), Long::plus)
                    expected.merge(merchant, p["captured"].asLong(), Long::plus)
                }
            }
            for (r in get("/v1/payments/$id/refunds").body) {
                states.merge("refund:" + r["state"].asString(), 1, Int::plus)
                if (r["state"].asString() == "refunded") {
                    expected.merge(customer, r["amount"].asLong(), Long::plus)
                    expected.merge(merchant, -r["amount"].asLong(), Long::plus)
                }
            }
        }
        for (id in payouts) {
            val p = get("/v1/payouts/$id").body
            states.merge("payout:" + p["state"].asString(), 1, Int::plus)
            if (p["state"].asString() == "settled") expected.merge(UUID.fromString(p["account"].asString()), -p["amount"].asLong(), Long::plus)
        }
        for ((account, balance) in expected) assertEquals(balance, balance(account), "account $account")
        val f = flaky as Flaky
        assertTrue(f.lostAfter.get() > 50, "answers were lost after the ledger acted: ${f.lostAfter.get()}")
        println("lost before: ${f.lostBefore.get()}, lost after: ${f.lostAfter.get()}; final states: ${states.toSortedMap()}")
    }
}
