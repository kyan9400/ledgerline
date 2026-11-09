package dev.ledgerline.payments

import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CardPaymentsTest : PaymentsTest() {

    private fun authorise(customer: UUID, merchant: UUID, amount: Long, key: String = UUID.randomUUID().toString()) =
        call("POST", "/v1/payments", mapOf("customer" to customer, "merchant" to merchant, "amount" to amount, "description" to "coffee"), key)

    @Test
    fun `an authorisation holds the money, and a smaller capture gives the rest back`() {
        val alice = open("alice", funded = 100_000)
        val shop = open("shop", "merchant")
        val auth = authorise(alice, shop, 30_000)
        assertEquals(201, auth.status, auth.raw)
        assertEquals("authorised", auth.body["state"].asString())
        assertEquals(70_000, balance(alice), "the hold has left the customer's balance")
        assertEquals(0, balance(shop), "but hasn't reached the merchant")

        val id = auth.body["id"].asString()
        val captured = call("POST", "/v1/payments/$id/capture", mapOf("amount" to 25_000))
        assertEquals(200, captured.status, captured.raw)
        assertEquals("captured", captured.body["state"].asString())
        assertEquals(25_000, captured.body["captured"].asLong())
        assertEquals(75_000, balance(alice), "the uncaptured 5,000 came back")
        assertEquals(25_000, balance(shop))

        val again = call("POST", "/v1/payments/$id/capture", mapOf("amount" to 1_000))
        assertEquals(409, again.status)
        assertEquals("invalid_state", again.body["code"].asString())
    }

    @Test
    fun `without the money an authorisation is declined, and nothing moves`() {
        val bob = open("bob", funded = 999)
        val shop = open("shop", "merchant")
        val auth = authorise(bob, shop, 1_000)
        assertEquals(201, auth.status, auth.raw)
        assertEquals("declined", auth.body["state"].asString())
        assertEquals("insufficient_funds", auth.body["declineReason"].asString())
        assertEquals(999, balance(bob))
    }

    @Test
    fun `a void gives the whole hold back, and a voided payment can't be captured`() {
        val carol = open("carol", funded = 10_000)
        val shop = open("shop", "merchant")
        val id = authorise(carol, shop, 4_000).body["id"].asString()
        assertEquals(6_000, balance(carol))
        val voided = call("POST", "/v1/payments/$id/void")
        assertEquals(200, voided.status, voided.raw)
        assertEquals("voided", voided.body["state"].asString())
        assertEquals("requested", voided.body["voidReason"].asString())
        assertEquals(10_000, balance(carol))
        assertEquals(409, call("POST", "/v1/payments/$id/capture").status)
    }

    @Test
    fun `an authorisation not captured in time expires, and the money comes back`() {
        val dan = open("dan", funded = 10_000)
        val shop = open("shop", "merchant")
        val id = authorise(dan, shop, 7_000).body["id"].asString()
        assertEquals(3_000, balance(dan))
        eventually(Duration.ofSeconds(20), "the hold expires (5 s in the tests)") { get("/v1/payments/$id").body["state"].asString() == "voided" }
        assertEquals("expired", get("/v1/payments/$id").body["voidReason"].asString())
        assertEquals(10_000, balance(dan))
    }

    @Test
    fun `refunds come in parts, never more than was captured, and fail if the merchant has no money`() {
        val erin = open("erin", funded = 50_000)
        val shop = open("shop", "merchant")
        val id = authorise(erin, shop, 20_000).body["id"].asString()
        assertEquals(200, call("POST", "/v1/payments/$id/capture").status)

        val first = call("POST", "/v1/payments/$id/refunds", mapOf("amount" to 5_000))
        assertEquals(201, first.status, first.raw)
        assertEquals("refunded", first.body["state"].asString())
        assertEquals(201, call("POST", "/v1/payments/$id/refunds", mapOf("amount" to 10_000)).status)
        val tooMuch = call("POST", "/v1/payments/$id/refunds", mapOf("amount" to 5_001))
        assertEquals(409, tooMuch.status)
        assertEquals("refund_exceeds_capture", tooMuch.body["code"].asString())
        assertEquals(5_000, tooMuch.body["refundable"].asLong())
        assertEquals(45_000, balance(erin))
        assertEquals(5_000, balance(shop))

        // The merchant spends its money; the next refund can't be paid, and the amount stays refundable.
        val elsewhere = open("elsewhere")
        assertEquals(201, ledger("POST", "/v1/transfers", mapOf("from" to shop, "to" to elsewhere, "amount" to 4_000)).status)
        val failed = call("POST", "/v1/payments/$id/refunds", mapOf("amount" to 5_000))
        assertEquals(201, failed.status, failed.raw)
        assertEquals("failed", failed.body["state"].asString())
        assertEquals("insufficient_funds", failed.body["failureReason"].asString())
        assertEquals(15_000, get("/v1/payments/$id").body["refunded"].asLong())
        assertEquals(201, call("POST", "/v1/payments/$id/refunds", mapOf("amount" to 1_000)).status)
        assertEquals(3, get("/v1/payments/$id/refunds").body.count { it["state"].asString() == "refunded" })
    }

    @Test
    fun `a retry with the same key gets the first answer, and pays once`() {
        val fay = open("fay", funded = 10_000)
        val shop = open("shop", "merchant")
        val key = UUID.randomUUID().toString()
        val first = authorise(fay, shop, 2_500, key)
        val retry = authorise(fay, shop, 2_500, key)
        assertFalse(first.replayed)
        assertTrue(retry.replayed)
        assertEquals(first.raw, retry.raw)
        assertEquals(7_500, balance(fay), "held once")

        val other = authorise(fay, shop, 2_600, key)
        assertEquals(422, other.status)
        assertEquals("idempotency_key_reused", other.body["code"].asString())
        assertEquals(400, call("POST", "/v1/payments", mapOf("customer" to fay, "merchant" to shop, "amount" to 1), key = null).status)
    }

    @Test
    fun `requests that make no sense are refused before anything is recorded`() {
        val gus = open("gus", funded = 10_000)
        val shop = open("shop", "merchant")
        assertEquals(422, authorise(gus, shop, 0).status)
        assertEquals(422, authorise(shop, gus, 100).status, "the customer must be a customer, the merchant a merchant")
        assertEquals(422, authorise(gus, UUID.randomUUID(), 100).status)
        assertEquals(404, get("/v1/payments/${UUID.randomUUID()}").status)
        assertEquals(10_000, balance(gus))
    }

    @Test
    fun `every change of state is a new version of the payment`() {
        val hal = open("hal", funded = 10_000)
        val shop = open("shop", "merchant")
        val id = authorise(hal, shop, 1_000).body["id"].asString()
        call("POST", "/v1/payments/$id/capture")
        val payment = get("/v1/payments/$id").body
        assertEquals("captured", payment["state"].asString())
        assertEquals(4, payment["version"].asInt(), "authorising, authorised, capturing, captured")
    }
}
