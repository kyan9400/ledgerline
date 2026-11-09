package dev.ledgerline.payments

import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.IOException
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * What a call to another service came to. Only [Done] and [Refused] are answers; [Unavailable] (a timeout,
 * a 5xx, a refused connection) says nothing about whether the call took effect, so the caller retries it
 * with the same idempotency key.
 */
sealed interface Outcome {
    data class Done(val status: Int, val body: JsonNode) : Outcome

    data class Refused(val status: Int, val code: String, val detail: String) : Outcome

    data class Unavailable(val reason: String) : Outcome
}

/** The ledger's HTTP API, as the payments service uses it. An interface, so tests can make it fail. */
interface LedgerClient {
    fun post(path: String, idempotencyKey: String, body: Any): Outcome

    fun get(path: String): Outcome
}

/** Calls over HTTP with a timeout; a slow ledger is treated as an unavailable one. */
class HttpService(private val base: String, private val timeout: Duration, private val json: JsonMapper) : LedgerClient {
    private val http = HttpClient.newBuilder().connectTimeout(timeout).build()

    override fun post(path: String, idempotencyKey: String, body: Any): Outcome = send(
        HttpRequest.newBuilder(URI("$base$path")).timeout(timeout)
            .header("Content-Type", "application/json").header("Idempotency-Key", idempotencyKey)
            .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body))).build(),
    )

    override fun get(path: String): Outcome = send(HttpRequest.newBuilder(URI("$base$path")).timeout(timeout).GET().build())

    fun put(path: String, body: Any): Outcome = send(
        HttpRequest.newBuilder(URI("$base$path")).timeout(timeout).header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body))).build(),
    )

    private fun send(req: HttpRequest): Outcome {
        val res = try {
            http.send(req, HttpResponse.BodyHandlers.ofString())
        } catch (e: IOException) {
            return Outcome.Unavailable(e.javaClass.simpleName + (e.message?.let { ": $it" } ?: ""))
        }
        val status = res.statusCode()
        if (status >= 500) return Outcome.Unavailable("HTTP $status")
        val body = try {
            json.readTree(res.body().ifEmpty { "{}" })
        } catch (e: Exception) {
            return Outcome.Unavailable("unreadable answer (HTTP $status): $e")
        }
        return if (status < 400) {
            Outcome.Done(status, body)
        } else {
            Outcome.Refused(status, body["code"]?.asString() ?: "http_$status", body["detail"]?.asString() ?: "")
        }
    }
}

data class LedgerAccount(val id: UUID, val code: String, val currency: String, val kind: String, val balance: Long)

/** What never changes about an account: enough to check a payment's accounts without asking the ledger. */
data class AccountFacts(val id: UUID, val currency: String, val kind: String)

/** The ledger operations the payments service needs, over a [LedgerClient]. */
class LedgerOps(val client: LedgerClient) {
    private val facts = ConcurrentHashMap<UUID, AccountFacts>()

    /** With its balance now: a call to the ledger every time. */
    fun account(id: UUID): LedgerAccount? = when (val o = client.get("/v1/accounts/$id")) {
        is Outcome.Done -> parse(o.body).also { facts[id] = AccountFacts(id, it.currency, it.kind) }
        is Outcome.Refused -> if (o.status == 404) null else throw Invalid("ledger: ${o.code}: ${o.detail}")
        is Outcome.Unavailable -> throw LedgerUnavailable(o.reason)
    }

    /**
     * An account's currency and kind, which the ledger never changes: asked once, then remembered (up to
     * 100,000 accounts; past that, the cache starts again).
     */
    fun facts(id: UUID): AccountFacts? = facts[id] ?: account(id)?.let {
        if (facts.size > 100_000) facts.clear()
        AccountFacts(it.id, it.currency, it.kind).also { f -> facts[id] = f }
    }

    fun accountByCode(code: String): LedgerAccount? =
        when (val o = client.get("/v1/accounts?code=${URLEncoder.encode(code, Charsets.UTF_8)}")) {
            is Outcome.Done -> parse(o.body)
            is Outcome.Refused -> if (o.status == 404) null else throw Invalid("ledger: ${o.code}: ${o.detail}")
            is Outcome.Unavailable -> throw LedgerUnavailable(o.reason)
        }

    fun openAccount(key: String, code: String, currency: String, allowNegative: Boolean): Outcome = client.post(
        "/v1/accounts", key, mapOf("code" to code, "currency" to currency, "kind" to "system", "allowNegative" to allowNegative),
    )

    fun transfer(key: String, from: UUID, to: UUID, amount: Long, description: String, metadata: Map<String, String>): Outcome =
        client.post(
            "/v1/transfers", key,
            mapOf("from" to from, "to" to to, "amount" to amount, "description" to description, "metadata" to metadata),
        )

    fun post(key: String, entries: List<Pair<UUID, Long>>, description: String, metadata: Map<String, String>): Outcome =
        client.post(
            "/v1/transactions", key,
            mapOf(
                "entries" to entries.map { (account, amount) -> mapOf("account" to account, "amount" to amount) },
                "description" to description,
                "metadata" to metadata,
            ),
        )

    private fun parse(n: JsonNode) = LedgerAccount(
        UUID.fromString(n["id"].asString()), n["code"].asString(), n["currency"].asString(), n["kind"].asString(), n["balance"].asLong(),
    )
}

/** The other bank, as the payout saga sees it. */
class BankClient(private val http: HttpService) {
    sealed interface Submitted {
        data object Accepted : Submitted

        data class Rejected(val code: String) : Submitted

        data class Unknown(val reason: String) : Submitted
    }

    sealed interface Status {
        data class Known(val status: String, val returnReason: String?) : Status

        data object Missing : Status

        data class Unknown(val reason: String) : Status
    }

    /** Idempotent by id at the bank: sending again after an unknown outcome is safe. */
    fun submit(id: UUID, amount: Long, currency: String, account: String): Submitted =
        when (val o = http.post("/transfers", id.toString(), mapOf("id" to id, "amount" to amount, "currency" to currency, "account" to account))) {
            is Outcome.Done -> Submitted.Accepted
            is Outcome.Refused -> if (o.status == 422) Submitted.Rejected(o.code) else Submitted.Unknown("HTTP ${o.status} ${o.code}")
            is Outcome.Unavailable -> Submitted.Unknown(o.reason)
        }

    fun status(id: UUID): Status = when (val o = http.get("/transfers/$id")) {
        is Outcome.Done -> Status.Known(o.body["status"].asString(), o.body["returnReason"]?.takeIf { !it.isNull }?.asString())
        is Outcome.Refused -> if (o.status == 404) Status.Missing else Status.Unknown("HTTP ${o.status} ${o.code}")
        is Outcome.Unavailable -> Status.Unknown(o.reason)
    }

    /** Everything the bank has received: for reconciliation. */
    fun received(): List<JsonNode> = when (val o = http.get("/transfers")) {
        is Outcome.Done -> o.body.toList()
        is Outcome.Refused -> error("the bank refused the listing: ${o.code}")
        is Outcome.Unavailable -> error("the bank is unavailable: ${o.reason}")
    }
}
