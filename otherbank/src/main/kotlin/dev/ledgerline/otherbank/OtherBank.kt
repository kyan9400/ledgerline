package dev.ledgerline.otherbank

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.KotlinModule
import tools.jackson.module.kotlin.readValue
import java.io.File
import java.net.InetSocketAddress
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/** A transfer as the other bank sees it: received, then settled or returned. */
data class Received(
    val id: String,
    val amount: Long,
    val currency: String,
    val account: String,
    val status: String, // pending, settled, returned
    val receivedAt: String,
    val updatedAt: String,
    val returnReason: String? = null,
)

data class Incoming(val id: String, val amount: Long, val currency: String, val account: String)

/**
 * How badly the bank behaves. Rates are probabilities per request.
 *
 * - `failRate`: answers 500 and does nothing.
 * - `slowRate`: does the work, then answers only after `slowMs` (past the caller's timeout): the caller
 *   can't tell whether the transfer arrived.
 * - `returnRate`: accepts a transfer, then later returns it (the account was frozen, say).
 * - A transfer settles or is returned between `settleMinMs` and `settleMaxMs` after it arrives.
 * - An account whose number starts with `closed-` is refused at once (422).
 */
data class Faults(
    val failRate: Double = 0.0,
    val slowRate: Double = 0.0,
    val slowMs: Long = 3_000,
    val returnRate: Double = 0.0,
    val settleMinMs: Long = 200,
    val settleMaxMs: Long = 2_000,
)

/**
 * The other bank: Ledgerline pays out to it. Its API is idempotent by the sender's transfer id, as a real
 * payment scheme's is by end-to-end id: a transfer sent twice is received once. What it has received is
 * kept in a journal file, so a restart forgets nothing.
 *
 * ```
 * POST /transfers         {id, amount, currency, account}  -> 202 pending | 200 (seen before) | 422 | 409
 * GET  /transfers/{id}    -> 200 {status: pending | settled | returned} | 404
 * GET  /transfers         everything received (for reconciliation; never faulty)
 * PUT  /admin/faults      set Faults
 * ```
 */
class OtherBank(port: Int, @Volatile var faults: Faults = Faults(), private val journal: File? = null) {
    private val json = JsonMapper.builder().addModule(KotlinModule.Builder().build()).build()
    private val transfers = ConcurrentHashMap<String, Received>()
    private val timers: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private val server = HttpServer.create(InetSocketAddress(port), 200)
    val port: Int get() = server.address.port

    init {
        journal?.takeIf { it.exists() }?.forEachLine { line ->
            if (line.isNotBlank()) json.readValue<Received>(line).let { transfers[it.id] = it }
        }
        // Whatever was pending when it stopped settles now.
        transfers.values.filter { it.status == "pending" }.forEach { schedule(it.id, 0) }
        server.executor = Executors.newVirtualThreadPerTaskExecutor()
        server.createContext("/") { ex ->
            try {
                route(ex)
            } catch (e: Exception) {
                reply(ex, 500, mapOf("code" to "internal", "detail" to e.toString()))
            }
        }
    }

    fun start(): OtherBank = apply { server.start() }

    fun stop() {
        server.stop(0)
        timers.shutdownNow()
    }

    fun received(): List<Received> = transfers.values.sortedBy { it.receivedAt }

    private fun route(ex: HttpExchange) {
        val path = ex.requestURI.path
        val method = ex.requestMethod
        when {
            path == "/health" -> reply(ex, 200, mapOf("status" to "UP"))
            path == "/admin/faults" && method == "PUT" -> {
                faults = json.readValue(ex.requestBody.readAllBytes())
                reply(ex, 200, faults)
            }
            path == "/admin/faults" -> reply(ex, 200, faults)
            path == "/transfers" && method == "GET" -> reply(ex, 200, received())
            path == "/transfers" && method == "POST" -> faulty(ex) { receive(json.readValue(ex.requestBody.readAllBytes())) }
            path.startsWith("/transfers/") && method == "GET" -> faulty(ex) {
                transfers[path.removePrefix("/transfers/")]?.let { 200 to it } ?: (404 to mapOf("code" to "not_found"))
            }
            else -> reply(ex, 404, mapOf("code" to "no_route"))
        }
    }

    private fun faulty(ex: HttpExchange, work: () -> Pair<Int, Any>) {
        val f = faults
        if (Random.nextDouble() < f.failRate) return reply(ex, 500, mapOf("code" to "unavailable"))
        val (status, body) = work()
        if (Random.nextDouble() < f.slowRate) Thread.sleep(f.slowMs)
        reply(ex, status, body)
    }

    @Synchronized
    private fun receive(t: Incoming): Pair<Int, Any> {
        transfers[t.id]?.let { seen ->
            val same = seen.amount == t.amount && seen.currency == t.currency && seen.account == t.account
            return if (same) 200 to seen else 409 to mapOf("code" to "id_reused", "detail" to "transfer ${t.id} was received with other details")
        }
        if (t.amount <= 0 || !Regex("^[A-Z]{3}$").matches(t.currency)) return 422 to mapOf("code" to "invalid")
        if (t.account.startsWith("closed-")) return 422 to mapOf("code" to "account_closed", "detail" to "account ${t.account} is closed")
        val now = Instant.now().toString()
        val r = Received(t.id, t.amount, t.currency, t.account, "pending", now, now)
        save(r)
        val f = faults
        schedule(t.id, f.settleMinMs + Random.nextLong(maxOf(1, f.settleMaxMs - f.settleMinMs)))
        return 202 to r
    }

    private fun schedule(id: String, delayMs: Long) {
        timers.schedule({
            val r = transfers.getValue(id)
            val now = Instant.now().toString()
            save(
                if (Random.nextDouble() < faults.returnRate) r.copy(status = "returned", updatedAt = now, returnReason = "account_frozen")
                else r.copy(status = "settled", updatedAt = now),
            )
        }, delayMs, TimeUnit.MILLISECONDS)
    }

    /** Journal first: if that fails, nothing is recorded, and the caller hears an error. */
    @Synchronized
    private fun save(r: Received) {
        journal?.appendText(json.writeValueAsString(r) + "\n")
        transfers[r.id] = r
    }

    private fun reply(ex: HttpExchange, status: Int, body: Any) {
        val bytes = json.writeValueAsBytes(body)
        ex.responseHeaders.add("Content-Type", "application/json")
        try {
            ex.sendResponseHeaders(status, bytes.size.toLong())
            ex.responseBody.use { it.write(bytes) }
        } catch (_: java.io.IOException) {
            // The caller gave up waiting (a slow answer): nothing to tell it.
        }
    }
}

fun main() {
    val env = System.getenv()
    val faults = Faults(
        failRate = env["FAIL_RATE"]?.toDouble() ?: 0.0,
        slowRate = env["SLOW_RATE"]?.toDouble() ?: 0.0,
        slowMs = env["SLOW_MS"]?.toLong() ?: 3_000,
        returnRate = env["RETURN_RATE"]?.toDouble() ?: 0.0,
        settleMinMs = env["SETTLE_MIN_MS"]?.toLong() ?: 200,
        settleMaxMs = env["SETTLE_MAX_MS"]?.toLong() ?: 2_000,
    )
    val bank = OtherBank(env["PORT"]?.toInt() ?: 8090, faults, env["JOURNAL"]?.let(::File)).start()
    println("otherbank listening on ${bank.port} with $faults")
}
