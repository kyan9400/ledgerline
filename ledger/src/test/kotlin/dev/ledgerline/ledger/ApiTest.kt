package dev.ledgerline.ledger

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.simple.JdbcClient
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class ApiTest : DatabaseTest() {
    @Value("\${local.server.port}")
    var port: Int = 0

    @Autowired
    lateinit var json: JsonMapper

    @Autowired
    lateinit var db: JdbcClient

    private val http = HttpClient.newHttpClient()

    data class Reply(val status: Int, val body: JsonNode, val replayed: Boolean)

    private fun call(method: String, path: String, body: Any? = null, key: String? = UUID.randomUUID().toString()): Reply {
        val req = HttpRequest.newBuilder(URI("http://127.0.0.1:$port$path"))
            .header("Content-Type", "application/json")
            .apply { if (key != null) header("Idempotency-Key", key) }
            .method(method, if (body == null) HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)))
            .build()
        val res = http.send(req, HttpResponse.BodyHandlers.ofString())
        return Reply(res.statusCode(), json.readTree(res.body().ifEmpty { "{}" }), res.headers().firstValue("Idempotent-Replayed").orElse("") == "true")
    }

    private fun open(name: String, kind: String = "customer", negative: Boolean = false): UUID =
        UUID.fromString(call("POST", "/v1/accounts", OpenAccount(code(name), "RUB", kind, negative)).body["id"].asString())

    private fun balance(id: UUID) = call("GET", "/v1/accounts/$id", key = null).body["balance"].asLong()

    @Test
    fun `money moves between accounts, and an overdraft is refused`() {
        val bank = open("issuance", "system", negative = true)
        val alice = open("alice")
        val bob = open("bob")
        val funded = call("POST", "/v1/transactions", PostingRequest(listOf(EntryRequest(bank, -100_000), EntryRequest(alice, 100_000)), "top-up"))
        assertEquals(201, funded.status)
        assertEquals(-100_000, balance(bank))

        val t = call("POST", "/v1/transfers", TransferRequest(alice, bob, 30_050, "rent"))
        assertEquals(201, t.status)
        assertEquals("transfer", t.body["kind"].asString())
        assertEquals(69_950, balance(alice))
        assertEquals(30_050, balance(bob))

        val refused = call("POST", "/v1/transfers", TransferRequest(bob, alice, 30_051))
        assertEquals(409, refused.status)
        assertEquals("insufficient_funds", refused.body["code"].asString())
        assertEquals(30_050, balance(bob))

        val statement = call("GET", "/v1/accounts/$alice/entries?limit=1", key = null).body
        assertEquals(-30_050, statement["entries"][0]["amount"].asLong())
        assertEquals(69_950, statement["entries"][0]["balanceAfter"].asLong())
        val older = call("GET", "/v1/accounts/$alice/entries?limit=1&before=${statement["next"].asInt()}", key = null).body
        assertEquals(100_000, older["entries"][0]["amount"].asLong())
    }

    @Test
    fun `a retry with the same key gets the same answer, and changes nothing`() {
        val bank = open("issuance", "system", negative = true)
        val alice = open("alice")
        val key = UUID.randomUUID().toString()
        val body = PostingRequest(listOf(EntryRequest(bank, -500), EntryRequest(alice, 500)))
        val first = call("POST", "/v1/transactions", body, key)
        val again = call("POST", "/v1/transactions", body, key)
        assertEquals(201, again.status)
        assertEquals(first.body, again.body)
        assertTrue(again.replayed && !first.replayed)
        assertEquals(500, balance(alice))

        // The same key with another request: refused, not replayed.
        val other = call("POST", "/v1/transactions", body.copy(description = "different"), key)
        assertEquals(422, other.status)
        assertEquals("idempotency_key_reused", other.body["code"].asString())

        // A refusal is an answer too: retried, it stays refused even once the money is there.
        val bob = open("bob")
        val k2 = UUID.randomUUID().toString()
        assertEquals(409, call("POST", "/v1/transfers", TransferRequest(bob, alice, 100), k2).status)
        call("POST", "/v1/transfers", TransferRequest(alice, bob, 200))
        val retried = call("POST", "/v1/transfers", TransferRequest(bob, alice, 100), k2)
        assertEquals(409, retried.status)
        assertTrue(retried.replayed)

        assertEquals(400, call("POST", "/v1/transfers", TransferRequest(bob, alice, 1), key = null).status)
    }

    @Test
    fun `what is refused`() {
        val bank = open("issuance", "system", negative = true)
        val alice = open("alice")
        val usd = UUID.fromString(call("POST", "/v1/accounts", OpenAccount(code("usd"), "USD", "customer")).body["id"].asString())

        fun codeOf(r: Reply) = r.body["code"].asString()
        assertEquals("invalid_request", codeOf(call("POST", "/v1/transactions", PostingRequest(listOf(EntryRequest(bank, -5), EntryRequest(alice, 4))))))
        assertEquals("invalid_request", codeOf(call("POST", "/v1/transactions", PostingRequest(listOf(EntryRequest(bank, -5), EntryRequest(usd, 5))))))
        assertEquals("invalid_request", codeOf(call("POST", "/v1/transfers", TransferRequest(alice, alice, 5))))
        assertEquals("invalid_request", codeOf(call("POST", "/v1/transfers", TransferRequest(bank, alice, 0))))
        assertEquals("invalid_request", codeOf(call("POST", "/v1/accounts", OpenAccount(code("x"), "XYZ", "customer"))))
        assertEquals("invalid_request", codeOf(call("POST", "/v1/accounts", OpenAccount(code("x"), "RUB", "customer", allowNegative = true))))
        assertEquals("invalid_request", codeOf(call("POST", "/v1/transactions", PostingRequest(listOf(EntryRequest(bank, Long.MIN_VALUE), EntryRequest(alice, -1))))))
        assertEquals(404, call("POST", "/v1/transfers", TransferRequest(UUID.randomUUID(), alice, 5)).status)
        assertEquals(404, call("GET", "/v1/accounts/${UUID.randomUUID()}", key = null).status)
        val dup = code("dup")
        call("POST", "/v1/accounts", OpenAccount(dup, "RUB", "customer"))
        assertEquals("account_exists", codeOf(call("POST", "/v1/accounts", OpenAccount(dup, "RUB", "customer"))))
    }

    @Test
    fun `a reversal undoes a transaction once, and only if the money is still there`() {
        val bank = open("issuance", "system", negative = true)
        val alice = open("alice")
        val bob = open("bob")
        call("POST", "/v1/transactions", PostingRequest(listOf(EntryRequest(bank, -1000), EntryRequest(alice, 1000))))
        val t = call("POST", "/v1/transfers", TransferRequest(alice, bob, 400)).body["id"].asString()

        val rev = call("POST", "/v1/transactions/$t/reversal", ReverseRequest("sent by mistake"))
        assertEquals(201, rev.status)
        assertEquals(t, rev.body["reverses"].asString())
        assertEquals(1000, balance(alice))
        assertEquals("already_reversed", call("POST", "/v1/transactions/$t/reversal").body["code"].asString())
        assertEquals("not_reversible", call("POST", "/v1/transactions/${rev.body["id"].asString()}/reversal").body["code"].asString())

        // Bob spent what he received: the reversal can't take it back.
        val t2 = call("POST", "/v1/transfers", TransferRequest(alice, bob, 300)).body["id"].asString()
        call("POST", "/v1/transfers", TransferRequest(bob, alice, 300))
        assertEquals("insufficient_funds", call("POST", "/v1/transactions/$t2/reversal").body["code"].asString())
    }

    @Test
    fun `the event log is append-only, and the books balance`() {
        val ex = assertFailsWith<DataAccessException> { db.sql("UPDATE events SET type = 'x' WHERE seq = (SELECT min(seq) FROM events)").update() }
        assertTrue(ex.mostSpecificCause.message!!.contains("append-only"))
        assertFailsWith<DataAccessException> { db.sql("DELETE FROM events WHERE seq = (SELECT min(seq) FROM events)").update() }
        val audit = call("GET", "/internal/audit", key = null)
        assertEquals(200, audit.status, audit.body.toString())
        assertTrue(audit.body["ok"].asBoolean())
        assertNotEquals(0, audit.body["totals"]["events"].asLong())
    }
}
