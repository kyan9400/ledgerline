package dev.ledgerline.checker

import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.File
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

/**
 * One request, as the client saw it: every answer it got for one idempotency key.
 */
class Op(val kind: String, val key: String, val parent: String?) {
    @Volatile var status = 0
    val ids: MutableSet<String> = java.util.concurrent.ConcurrentHashMap.newKeySet()
    val attempts = AtomicInteger()
    @Volatile var resolved = false
    @Volatile var ms = 0L
    val errors: MutableMap<String, Int> = java.util.concurrent.ConcurrentHashMap()
    @Volatile var duplicated = false

    fun log() = linkedMapOf(
        "kind" to kind, "key" to key, "parent" to parent, "status" to status, "ids" to ids.toList(),
        "attempts" to attempts.get(), "resolved" to resolved, "ms" to ms, "errors" to errors, "duplicated" to duplicated,
    )
}

/**
 * A client that keeps paying while things break, and behaves as a client should: every write has an
 * idempotency key, and a request that gets no answer (a timeout, a refused connection, a 5xx or a 503)
 * is sent again with the same key until it gets one, or the deadline passes. Some requests are sent
 * twice at once, as a client whose first try only seemed to fail would.
 *
 * Card payments (authorise, then capture all or part, void or leave; refunds of captured ones) and
 * payouts, at a steady rate. Everything it sent and every answer go to the log, for `checker verify`.
 *
 *   checker workload /out/log.json   (env: PAYMENTS, LEDGER, DURATION_S=120, RATE=20, CUSTOMERS=100,
 *                                     MERCHANTS=10, SEED, DEADLINE_S=300, DUPLICATES=0.1)
 */
fun workload(env: Map<String, String>, out: String): Pair<Map<String, Any>, Boolean> {
    val payments = env["PAYMENTS"] ?: "http://payments:8080"
    val ledger = env["LEDGER"] ?: "http://ledger:8080"
    val duration = env["DURATION_S"]?.toLong() ?: 120
    val rate = env["RATE"]?.toDouble() ?: 20.0
    val seed = env["SEED"]?.toLong() ?: System.currentTimeMillis()
    val duplicates = env["DUPLICATES"]?.toDouble() ?: 0.1
    val random = Random(seed)
    val json = JsonMapper.builder().build()
    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()
    val run = "chaos-${System.currentTimeMillis().toString(36)}" // unique: account codes are once only
    val ops = ConcurrentLinkedQueue<Op>()

    // Setup, before anything breaks: customers with money, merchants without.
    val customers = (1..(env["CUSTOMERS"]?.toInt() ?: 100)).map { "$run:c$it" }
    val merchants = (1..(env["MERCHANTS"]?.toInt() ?: 10)).map { "$run:m$it" }
    val funding = 10_000_000L
    val c = Client(http, json, System.nanoTime() + Duration.ofMinutes(5).toNanos())
    val bank = c.must("POST", "$ledger/v1/accounts", mapOf("code" to "$run:bank", "currency" to "RUB", "kind" to "system", "allowNegative" to true))["id"].asString()
    val customerIds = customers.map { code ->
        val id = c.must("POST", "$ledger/v1/accounts", mapOf("code" to code, "currency" to "RUB", "kind" to "customer"))["id"].asString()
        c.must("POST", "$ledger/v1/transfers", mapOf("from" to bank, "to" to id, "amount" to funding))
        id
    }
    val merchantIds = merchants.map { code -> c.must("POST", "$ledger/v1/accounts", mapOf("code" to code, "currency" to "RUB", "kind" to "merchant"))["id"].asString() }

    env["READY_FILE"]?.let { File(it).writeText("ready") } // setup done: whoever breaks things may start
    val end = System.nanoTime() + Duration.ofSeconds(duration).toNanos()
    val deadline = end + Duration.ofSeconds(env["DEADLINE_S"]?.toLong() ?: 300).toNanos()
    val client = Client(http, json, deadline)
    val executor = Executors.newVirtualThreadPerTaskExecutor()
    val started = AtomicInteger()

    fun send(op: Op, method: String, path: String, body: Any?): JsonNode? {
        ops += op
        val twice = synchronized(random) { random.nextDouble() } < duplicates
        if (!twice) return client.send(op, method, "$payments$path", body)
        op.duplicated = true
        // Its own thread: the flows' executor stops taking work when the load ends, and flows still
        // retrying after an outage go on past that.
        var theirs: JsonNode? = null
        val other = Thread.ofVirtual().start { theirs = client.send(op, method, "$payments$path", body) }
        val mine = client.send(op, method, "$payments$path", body)
        other.join()
        return theirs ?: mine
    }

    /**
     * Polls until the resource leaves its step state (or the deadline passes), backing off as it goes:
     * after an outage, hundreds of clients polling five times a second would crowd out the writes.
     */
    fun settle(path: String, stepState: String): JsonNode? {
        var wait = 200L
        while (System.nanoTime() < deadline) {
            val now = client.get("$payments$path")
            if (now != null && now["state"].asString() != stepState) return now
            Thread.sleep(wait + Random.nextLong(wait))
            wait = (wait * 2).coerceAtMost(4_000)
        }
        return null
    }

    fun cardPayment(r: Random) {
        val amount = 100L + r.nextLong(50_000)
        val auth = Op("authorise", UUID.randomUUID().toString(), null)
        val a = send(auth, "POST", "/v1/payments", mapOf("customer" to customerIds[r.nextInt(customerIds.size)], "merchant" to merchantIds[r.nextInt(merchantIds.size)], "amount" to amount)) ?: return
        if (auth.status !in 200..299) return
        val id = a["id"].asString()
        val p = settle("/v1/payments/$id", "authorising") ?: return
        if (p["state"].asString() != "authorised") return
        val roll = r.nextDouble()
        when {
            roll < 0.15 -> send(Op("void", UUID.randomUUID().toString(), id), "POST", "/v1/payments/$id/void", null)
            roll < 0.25 -> return // left authorised
            else -> {
                val partial = roll < 0.45
                val cap = Op("capture", UUID.randomUUID().toString(), id)
                send(cap, "POST", "/v1/payments/$id/capture", if (partial) mapOf("amount" to amount / 2 + 1) else emptyMap<String, Any>())
                val done = settle("/v1/payments/$id", "capturing") ?: return
                if (done["state"].asString() == "captured" && r.nextDouble() < 0.35) {
                    repeat(1 + r.nextInt(2)) {
                        send(Op("refund", UUID.randomUUID().toString(), id), "POST", "/v1/payments/$id/refunds", mapOf("amount" to maxOf(1, done["captured"].asLong() / 3)))
                    }
                }
            }
        }
    }

    fun payout(r: Random) {
        val beneficiary = if (r.nextDouble() < 0.05) "closed-${r.nextInt(1000)}" else "ru-${r.nextInt(1_000_000_000)}"
        send(
            Op("payout", UUID.randomUUID().toString(), null), "POST", "/v1/payouts",
            mapOf("account" to customerIds[r.nextInt(customerIds.size)], "amount" to 100L + r.nextLong(5_000), "bank" to "otherbank", "beneficiary" to beneficiary),
        )
    }

    val interval = (1e9 / rate).toLong()
    var next = System.nanoTime()
    while (next < end) {
        val flowSeed = synchronized(random) { random.nextLong() }
        executor.execute {
            val r = Random(flowSeed)
            started.incrementAndGet()
            try {
                if (r.nextDouble() < 0.8) cardPayment(r) else payout(r)
            } catch (e: Exception) {
                System.err.println("flow failed: $e")
            }
        }
        next += interval
        val wait = next - System.nanoTime()
        if (wait > 0) Thread.sleep(wait / 1_000_000, (wait % 1_000_000).toInt())
    }
    executor.shutdown()
    executor.awaitTermination(deadline - System.nanoTime() + 30_000_000_000, TimeUnit.NANOSECONDS)

    val all = ops.toList()
    val log = linkedMapOf(
        "run" to run, "seed" to seed, "duration_s" to duration, "rate" to rate,
        "customers" to customerIds, "merchants" to merchantIds, "funding" to funding,
        "ops" to all.map { it.log() },
    )
    File(out).writeText(json.writeValueAsString(log))
    val times = all.filter { it.resolved }.map { it.ms }.sorted()
    fun pct(q: Double) = if (times.isEmpty()) 0 else times[((times.size - 1) * q).toInt()]
    val summary = linkedMapOf<String, Any>(
        "run" to run,
        "flows" to started.get(),
        "requests" to all.size,
        "by_kind_and_status" to all.groupingBy { "${it.kind} ${it.status}" }.eachCount().toSortedMap(),
        "sent_twice_at_once" to all.count { it.duplicated },
        "attempts" to all.sumOf { it.attempts.get() },
        "requests_retried" to all.count { it.attempts.get() > 1 },
        "unresolved" to all.count { !it.resolved },
        "errors_seen" to all.flatMap { it.errors.entries }.groupBy({ it.key }, { it.value }).mapValues { it.value.sum() }.toSortedMap(),
        "ms_to_answer" to mapOf("p50" to pct(0.5), "p95" to pct(0.95), "p99" to pct(0.99), "max" to (times.lastOrNull() ?: 0)),
        "log" to out,
    )
    return summary to (all.none { !it.resolved })
}

/** Sends until it gets an answer; a 2xx or 4xx is an answer, nothing else is. */
class Client(private val http: HttpClient, private val json: JsonMapper, private val deadline: Long) {

    fun send(op: Op, method: String, url: String, body: Any?): JsonNode? {
        val started = System.nanoTime()
        var backoff = 100L
        while (System.nanoTime() < deadline) {
            op.attempts.incrementAndGet()
            val req = HttpRequest.newBuilder(URI(url)).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json").header("Idempotency-Key", op.key)
                .method(method, HttpRequest.BodyPublishers.ofString(if (body == null) "" else json.writeValueAsString(body))).build()
            val reason = try {
                val res = http.send(req, HttpResponse.BodyHandlers.ofString())
                if (res.statusCode() in 200..499) {
                    val node = json.readTree(res.body().ifEmpty { "{}" })
                    synchronized(op) {
                        op.status = res.statusCode()
                        node["id"]?.asString()?.let { if (res.statusCode() < 300) op.ids += it }
                        if (!op.resolved) op.ms = (System.nanoTime() - started) / 1_000_000
                        op.resolved = true
                    }
                    return node
                }
                "http_${res.statusCode()}"
            } catch (e: IOException) {
                e.javaClass.simpleName
            }
            op.errors.merge(reason, 1, Int::plus)
            Thread.sleep(backoff + Random.nextLong(backoff))
            backoff = (backoff * 2).coerceAtMost(4_000)
        }
        return null
    }

    /** A read: retried the same way, nothing logged. */
    fun get(url: String): JsonNode? {
        val res = try {
            http.send(HttpRequest.newBuilder(URI(url)).timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString())
        } catch (_: IOException) {
            return null
        }
        return if (res.statusCode() == 200) json.readTree(res.body()) else null
    }

    /** Setup: must succeed (nothing is broken yet). */
    fun must(method: String, url: String, body: Any): JsonNode {
        val op = Op("setup", UUID.randomUUID().toString(), null)
        val node = send(op, method, url, body) ?: error("setup: no answer from $url")
        check(op.status in 200..299) { "setup: $url answered ${op.status}: $node" }
        return node
    }
}
