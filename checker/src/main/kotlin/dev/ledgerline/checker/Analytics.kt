package dev.ledgerline.checker

import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Proves that ClickHouse, fed only from Kafka, agrees with the services' databases:
 *
 * - the same events: as many distinct ledger events as the ledger's event log has;
 * - the same balances: every account's postings in ClickHouse sum to its balance in the ledger, and its
 *   last version is the ledger's;
 * - the same payments, refunds and payouts: each in the same state, at the same version;
 * - the same money: what each merchant has captured.
 *
 * ClickHouse may have received an event more than once (delivery is at least once); its tables keep
 * one row per event, read with FINAL. It waits (up to two minutes) for ClickHouse to catch up first.
 *
 *   checker analytics   (env: CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, and those of check)
 */
fun analytics(env: Map<String, String>): Pair<Map<String, Any>, Boolean> {
    val ch = ClickHouse(env["CLICKHOUSE_URL"] ?: "http://clickhouse:8123", env.getValue("CLICKHOUSE_USER"), env.getValue("CLICKHOUSE_PASSWORD"))
    val report = linkedMapOf<String, Any>()
    val violations = linkedMapOf<String, Long>()

    val ledgerEvents = connect(env.getValue("LEDGER_DB"), env.getValue("LEDGER_DB_USER"), env.getValue("LEDGER_DB_PASSWORD")).use {
        count(it, "SELECT count(*) FROM events")
    }
    val paymentEvents = connect(env.getValue("PAYMENTS_DB"), env.getValue("PAYMENTS_DB_USER"), env.getValue("PAYMENTS_DB_PASSWORD")).use {
        count(it, "SELECT count(*) FROM events")
    }
    val deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos()
    var seen = Pair(0L, 0L)
    while (System.nanoTime() < deadline) {
        seen = ch.long("SELECT count() FROM analytics.ledger_events FINAL") to ch.long("SELECT count() FROM analytics.payment_events FINAL")
        if (seen.first >= ledgerEvents && seen.second >= paymentEvents) break
        Thread.sleep(1_000)
    }
    report["events"] = linkedMapOf(
        "ledger_in_postgres" to ledgerEvents, "ledger_in_clickhouse" to seen.first,
        "ledger_rows_before_dedup" to ch.long("SELECT count() FROM analytics.ledger_events"),
        "payments_in_postgres" to paymentEvents, "payments_in_clickhouse" to seen.second,
        "payments_rows_before_dedup" to ch.long("SELECT count() FROM analytics.payment_events"),
    )
    violations["ledger_events_missing_or_extra"] = kotlin.math.abs(ledgerEvents - seen.first)
    violations["payment_events_missing_or_extra"] = kotlin.math.abs(paymentEvents - seen.second)

    // Balances.
    val chBalances = ch.rows("SELECT toString(account), sum(amount), max(version) FROM analytics.postings FINAL GROUP BY account")
        .associate { it[0] to (it[1].toLong() to it[2].toInt()) }
    val pgBalances = connect(env.getValue("LEDGER_DB"), env.getValue("LEDGER_DB_USER"), env.getValue("LEDGER_DB_PASSWORD")).use { db ->
        query(db, "SELECT account_id::text, balance, version FROM balances") { it.getString(1) to (it.getLong(2) to it.getInt(3)) }.toMap()
    }
    // An account with no postings has balance 0 and version 1 (its opening) in the ledger, and no rows here.
    violations["balances_not_matching"] = pgBalances.count { (account, bv) ->
        val c = chBalances[account]
        if (c == null) bv.first != 0L || bv.second != 1 else c.first != bv.first || c.second != bv.second
    }.toLong() + chBalances.keys.count { it !in pgBalances }
    report["accounts"] = pgBalances.size

    // Payments, refunds, payouts.
    val chLatest = ch.rows("SELECT entity, toString(id), version, state FROM analytics.latest FINAL")
        .associate { "${it[0]}-${it[1]}" to (it[2].toInt() to it[3]) }
    val pgLatest = connect(env.getValue("PAYMENTS_DB"), env.getValue("PAYMENTS_DB_USER"), env.getValue("PAYMENTS_DB_PASSWORD")).use { db ->
        listOf("payment" to "payments", "refund" to "refunds", "payout" to "payouts").flatMap { (entity, table) ->
            query(db, "SELECT id::text, version, state FROM $table") { "$entity-${it.getString(1)}" to (it.getInt(2) to it.getString(3)) }
        }.toMap()
    }
    violations["resources_not_matching"] = pgLatest.count { (k, v) -> chLatest[k] != v }.toLong() + chLatest.keys.count { it !in pgLatest }
    report["resources"] = pgLatest.size

    // Money captured, per merchant.
    val chCaptured = ch.rows("SELECT toString(merchant), sum(captured) FROM analytics.latest FINAL WHERE entity = 'payment' GROUP BY merchant")
        .associate { it[0] to it[1].toLong() }.filterValues { it != 0L }
    val pgCaptured = connect(env.getValue("PAYMENTS_DB"), env.getValue("PAYMENTS_DB_USER"), env.getValue("PAYMENTS_DB_PASSWORD")).use { db ->
        query(db, "SELECT merchant::text, sum(captured) FROM payments GROUP BY merchant HAVING sum(captured) <> 0") { it.getString(1) to it.getLong(2) }.toMap()
    }
    violations["merchants_captured_not_matching"] = (pgCaptured.keys + chCaptured.keys).count { pgCaptured[it] != chCaptured[it] }.toLong()
    report["captured_total"] = pgCaptured.values.sum()

    val ok = violations.values.all { it == 0L }
    return (linkedMapOf<String, Any>("ok" to ok, "violations" to violations) + report) to ok
}

/** ClickHouse over HTTP, tab-separated. */
class ClickHouse(private val url: String, private val user: String, private val password: String) {
    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

    fun rows(sql: String): List<List<String>> {
        val req = HttpRequest.newBuilder(URI("$url/?default_format=TabSeparated&query=" + URLEncoder.encode(sql, Charsets.UTF_8)))
            .timeout(Duration.ofSeconds(60)).header("X-ClickHouse-User", user).header("X-ClickHouse-Key", password).GET().build()
        val res = http.send(req, HttpResponse.BodyHandlers.ofString())
        check(res.statusCode() == 200) { "ClickHouse: ${res.statusCode()} ${res.body().take(300)}" }
        return res.body().lineSequence().filter { it.isNotEmpty() }.map { it.split('\t') }.toList()
    }

    fun long(sql: String): Long = rows(sql).single().single().toLong()
}
