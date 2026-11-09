package dev.ledgerline.checker

import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.Connection
import java.time.Duration

/**
 * After a workload (and whatever broke during it), proves from the client's log, the ledger's and the
 * payments service's databases, and Kafka, that nothing was lost and nothing was done twice:
 *
 * - **the client:** every request got an answer in the end, and every answer for one key named the same
 *   resource (a retry never made a second payment);
 * - **nothing lost, nothing extra:** the payments, refunds and payouts the service has for the run's
 *   accounts are exactly the ones the client was told about;
 * - **each one moved money exactly as its final state says:** the ledger has exactly the postings its
 *   state implies, once each (a captured payment: one authorisation and one capture; a settled payout: one
 *   reservation and one settlement; a declined payment: none);
 * - **the balances:** every customer and merchant has exactly what those final states imply;
 * - **the services' own checks:** the payments reconciliation and the ledger's audit hold;
 * - **Kafka:** the books rebuild from it (checker check).
 *
 *   checker verify /out/log.json   (env: PAYMENTS, LEDGER, and those of checker check)
 */
fun verify(env: Map<String, String>, logFile: String): Pair<Map<String, Any>, Boolean> {
    val json = JsonMapper.builder().build()
    val log = json.readTree(File(logFile))
    val payments = env["PAYMENTS"] ?: "http://payments:8080"
    val ledger = env["LEDGER"] ?: "http://ledger:8080"
    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()
    val ops = log["ops"].items()
    val report = linkedMapOf<String, Any>()
    val violations = linkedMapOf<String, Long>()

    // The client.
    violations["requests_never_answered"] = ops.count { !it["resolved"].asBoolean() }.toLong()
    violations["keys_answered_with_two_resources"] = ops.count { it["ids"].size() > 1 }.toLong()

    // Wait until nothing is in progress, then the services' own checks.
    val deadline = System.nanoTime() + Duration.ofMinutes(10).toNanos()
    var reconcile: JsonNode? = null
    var waited = 0L
    val since = System.nanoTime()
    while (System.nanoTime() < deadline) {
        reconcile = get(http, json, "$payments/internal/reconcile")
        if (reconcile?.get("quiet")?.asBoolean() == true) break
        Thread.sleep(1_000)
    }
    waited = (System.nanoTime() - since) / 1_000_000_000
    report["seconds_until_quiet"] = waited
    violations["reconciliation_failed"] = if (reconcile?.get("ok")?.asBoolean() == true) 0 else 1
    report["reconciliation"] = reconcile ?: "no answer"
    val audit = get(http, json, "$ledger/internal/audit")
    violations["ledger_audit_failed"] = if (audit?.get("ok")?.asBoolean() == true) 0 else 1
    report["ledger_audit_violations"] = audit?.get("violations") ?: "no answer"

    val customers = log["customers"].items().map { it.asString() }
    val merchants = log["merchants"].items().map { it.asString() }
    val accounts = customers + merchants
    val told = mapOf(
        "payments" to ops.filter { it["kind"].asString() == "authorise" }.flatMap { o -> o["ids"].items().map { it.asString() } }.toSet(),
        "refunds" to ops.filter { it["kind"].asString() == "refund" }.flatMap { o -> o["ids"].items().map { it.asString() } }.toSet(),
        "payouts" to ops.filter { it["kind"].asString() == "payout" }.flatMap { o -> o["ids"].items().map { it.asString() } }.toSet(),
    )

    data class Payment(val id: String, val customer: String, val merchant: String, val state: String, val amount: Long, val captured: Long)
    data class Refund(val id: String, val payment: String, val state: String, val amount: Long)
    data class Payout(val id: String, val account: String, val state: String, val amount: Long)

    val (ps, rs, pos) = connect(env.getValue("PAYMENTS_DB"), env.getValue("PAYMENTS_DB_USER"), env.getValue("PAYMENTS_DB_PASSWORD")).use { db ->
        val list = accounts.joinToString(",") { "'$it'" }
        Triple(
            rows(db, "SELECT id, customer, merchant, state, amount, captured FROM payments WHERE customer::text IN ($list)") {
                Payment(it.getString(1), it.getString(2), it.getString(3), it.getString(4), it.getLong(5), it.getLong(6))
            },
            rows(db, "SELECT r.id, r.payment_id, r.state, r.amount FROM refunds r JOIN payments p ON p.id = r.payment_id WHERE p.customer::text IN ($list)") {
                Refund(it.getString(1), it.getString(2), it.getString(3), it.getLong(4))
            },
            rows(db, "SELECT id, account, state, amount FROM payouts WHERE account::text IN ($list)") {
                Payout(it.getString(1), it.getString(2), it.getString(3), it.getLong(4))
            },
        )
    }
    val held = mapOf("payments" to ps.map { it.id }.toSet(), "refunds" to rs.map { it.id }.toSet(), "payouts" to pos.map { it.id }.toSet())
    for (kind in held.keys) {
        violations["${kind}_the_client_never_heard_of"] = (held.getValue(kind) - told.getValue(kind)).size.toLong()
        violations["${kind}_the_client_was_told_of_but_missing"] = (told.getValue(kind) - held.getValue(kind)).size.toLong()
    }

    // The ledger: every posting the payments service made, by what it was for.
    val (postings, balances) = connect(env.getValue("LEDGER_DB"), env.getValue("LEDGER_DB_USER"), env.getValue("LEDGER_DB_PASSWORD")).use { db ->
        val byResource = HashMap<String, MutableList<String>>()
        rows(db, "SELECT coalesce(metadata->>'refund', metadata->>'payout', metadata->>'payment'), description FROM transactions WHERE jsonb_exists_any(metadata, array['payment', 'payout'])") {
            it.getString(1) to it.getString(2)
        }.forEach { (id, what) -> byResource.getOrPut(id) { mutableListOf() } += what }
        val list = accounts.joinToString(",") { "'$it'" }
        byResource to rows(db, "SELECT account_id::text, balance FROM balances WHERE account_id::text IN ($list)") { it.getString(1) to it.getLong(2) }.toMap()
    }
    fun expect(id: String, wanted: List<String>) = postings[id].orEmpty().sorted() == wanted.sorted()
    var wrongEffects = 0L
    val examples = mutableListOf<String>()
    fun wrong(what: String) {
        wrongEffects++
        if (examples.size < 10) examples += what
    }
    for (p in ps) {
        val wanted = when (p.state) {
            "declined" -> emptyList()
            "authorised" -> listOf("card authorisation")
            "captured" -> listOf("card authorisation", "card capture")
            "voided" -> listOf("card authorisation", "card authorisation voided")
            else -> listOf("in progress: ${p.state}")
        }
        if (!expect(p.id, wanted)) wrong("payment ${p.id} ${p.state}: ${postings[p.id]}")
    }
    for (r in rs) {
        val wanted = if (r.state == "refunded") listOf("card refund") else emptyList()
        if (!expect(r.id, wanted)) wrong("refund ${r.id} ${r.state}: ${postings[r.id]}")
    }
    for (p in pos) {
        val wanted = when (p.state) {
            "refused" -> emptyList()
            "settled" -> listOf("payout reserved", "payout settled")
            "returned" -> listOf("payout reserved", "payout returned")
            else -> listOf("in progress: ${p.state}")
        }
        if (!expect(p.id, wanted)) wrong("payout ${p.id} ${p.state}: ${postings[p.id]}")
    }
    violations["postings_not_matching_final_state"] = wrongEffects
    if (examples.isNotEmpty()) report["examples"] = examples

    // The balances those final states imply.
    val expected = HashMap<String, Long>()
    customers.forEach { expected[it] = log["funding"].asLong() }
    merchants.forEach { expected[it] = 0 }
    for (p in ps) {
        when (p.state) {
            "authorised" -> expected.merge(p.customer, -p.amount, Long::plus)
            "captured" -> {
                expected.merge(p.customer, -p.captured, Long::plus)
                expected.merge(p.merchant, p.captured, Long::plus)
            }
        }
    }
    val paymentById = ps.associateBy { it.id }
    for (r in rs.filter { it.state == "refunded" }) {
        val p = paymentById.getValue(r.payment)
        expected.merge(p.customer, r.amount, Long::plus)
        expected.merge(p.merchant, -r.amount, Long::plus)
    }
    for (p in pos.filter { it.state == "settled" }) expected.merge(p.account, -p.amount, Long::plus)
    violations["balances_not_as_the_final_states_say"] = expected.count { (account, balance) -> balances[account] != balance }.toLong()

    report["resources"] = linkedMapOf(
        "payments" to ps.groupingBy { it.state }.eachCount().toSortedMap(),
        "refunds" to rs.groupingBy { it.state }.eachCount().toSortedMap(),
        "payouts" to pos.groupingBy { it.state }.eachCount().toSortedMap(),
    )

    // Kafka.
    val (kafka, kafkaOk) = check(env)
    violations["kafka_rebuild_failed"] = if (kafkaOk) 0 else 1
    report["kafka"] = kafka

    val ok = violations.values.all { it == 0L }
    return (linkedMapOf<String, Any>("ok" to ok, "violations" to violations) + report) to ok
}

private fun get(http: HttpClient, json: JsonMapper, url: String): JsonNode? = try {
    val res = http.send(HttpRequest.newBuilder(URI(url)).timeout(Duration.ofSeconds(60)).GET().build(), HttpResponse.BodyHandlers.ofString())
    if (res.statusCode() in setOf(200, 409, 500)) json.readTree(res.body()) else null
} catch (_: Exception) {
    null
}

/** A JSON array's items. */
fun JsonNode.items(): List<JsonNode> = (0 until size()).map { get(it) }

private fun <T> rows(db: Connection, sql: String, row: (java.sql.ResultSet) -> T): List<T> = query(db, sql, row)
