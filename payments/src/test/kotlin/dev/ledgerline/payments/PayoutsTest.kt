package dev.ledgerline.payments

import dev.ledgerline.otherbank.Faults
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PayoutsTest : PaymentsTest() {
    private val calm = Faults(settleMinMs = 50, settleMaxMs = 300)

    @AfterEach
    fun calmTheBank() {
        World.bank.faults = calm
    }

    private fun payout(account: UUID, amount: Long, beneficiary: String = "ru-40817810-${Random.nextInt(1_000_000)}", key: String = UUID.randomUUID().toString()) =
        call("POST", "/v1/payouts", mapOf("account" to account, "amount" to amount, "bank" to "otherbank", "beneficiary" to beneficiary), key)

    private fun settle(id: String): String {
        eventually(Duration.ofSeconds(30), "payout $id finishes") { get("/v1/payouts/$id").body["state"].asString() !in PAYOUT_STEPS }
        return get("/v1/payouts/$id").body["state"].asString()
    }

    @Test
    fun `a payout reserves the money, the bank settles it, and it leaves through settlement`() {
        val ann = open("ann", funded = 100_000)
        val p = payout(ann, 40_000)
        assertEquals(202, p.status, p.raw)
        assertEquals("submitting", p.body["state"].asString(), "the money is reserved at once")
        assertEquals(60_000, balance(ann))
        val id = p.body["id"].asString()
        assertEquals("settled", settle(id))
        assertEquals(60_000, balance(ann))
        assertEquals("settled", World.bank.received().single { it.id == id }.status)
    }

    @Test
    fun `a payout the bank refuses comes back`() {
        val ben = open("ben", funded = 10_000)
        val id = payout(ben, 3_000, beneficiary = "closed-12345").body["id"].asString()
        assertEquals("returned", settle(id))
        assertEquals("bank_rejected:account_closed", get("/v1/payouts/$id").body["failureReason"].asString())
        assertEquals(10_000, balance(ben))
    }

    @Test
    fun `without the money a payout is refused at once`() {
        val cat = open("cat", funded = 100)
        val p = payout(cat, 101)
        assertEquals("refused", p.body["state"].asString(), p.raw)
        assertEquals("insufficient_funds", p.body["failureReason"].asString())
        assertEquals(100, balance(cat))
    }

    /**
     * The bank fails a third of its answers, answers a fifth only after the payments service has given up
     * waiting (but did the work), and returns a fifth of what it accepted. Every payout must still end
     * once: settled (the money gone, the bank has it once) or returned (the money back).
     */
    @Test
    fun `against a bank that fails, answers late and returns payments, every payout ends exactly once`() {
        World.bank.faults = Faults(failRate = 0.3, slowRate = 0.2, slowMs = 1_500, returnRate = 0.2, settleMinMs = 50, settleMaxMs = 500)
        val customers = (1..10).map { open("payer$it", funded = 1_000_000) }
        val pool = Executors.newFixedThreadPool(16)
        val ids = java.util.concurrent.ConcurrentLinkedQueue<String>()
        repeat(150) { i ->
            pool.execute {
                val key = UUID.randomUUID().toString()
                val beneficiary = if (i % 10 == 0) "closed-$i" else "ru-$i"
                var r = payout(customers[i % customers.size], 1_000L + i, beneficiary, key)
                while (r.status == 503) r = payout(customers[i % customers.size], 1_000L + i, beneficiary, key)
                assertEquals(202, r.status, r.raw)
                ids += r.body["id"].asString()
            }
        }
        pool.shutdown()
        assertTrue(pool.awaitTermination(2, TimeUnit.MINUTES))
        World.bank.faults = calm // the stragglers get through; the bank's earlier answers still stand
        val report = reconciled()
        assertEquals(0, report["violations"]["bank_records_not_matching"].asLong())

        val finals = ids.map { get("/v1/payouts/$it").body }
        assertEquals(150, finals.size)
        assertTrue(finals.all { it["state"].asString() in setOf("settled", "returned") }, finals.map { it["state"] }.toString())
        val byBank = World.bank.received().associateBy { it.id }
        for (p in finals) {
            val theirs = byBank[p["id"].asString()]
            when {
                p["state"].asString() == "settled" -> assertEquals("settled", theirs!!.status)
                p["failureReason"].asString().startsWith("bank_returned") -> assertEquals("returned", theirs!!.status)
                else -> assertEquals(null, theirs, "a rejected payout never reached the bank")
            }
        }
        for ((i, c) in customers.withIndex()) {
            val settled = finals.filter { it["account"].asString() == c.toString() && it["state"].asString() == "settled" }.sumOf { it["amount"].asLong() }
            assertEquals(1_000_000 - settled, balance(c), "payer${i + 1}: only settled payouts left the account")
        }
        println("payouts: " + finals.groupingBy { it["state"].asString() + ":" + (it["failureReason"]?.asString() ?: "") }.eachCount())
    }
}
