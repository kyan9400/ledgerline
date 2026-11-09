package dev.ledgerline.payments

import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID

data class ReconcileReport(
    /** Every check holds. */
    val ok: Boolean,
    /** Nothing in progress: the checks that compare with the ledger and the bank ran. */
    val quiet: Boolean,
    val inProgress: Map<String, Long>,
    /** For each check, how many things are wrong (0 when it holds). */
    val violations: Map<String, Long>,
    val totals: Map<String, Long>,
)

/**
 * Checks the payments service's records against the ledger's balances and the other bank's records:
 *
 * - each currency's holds accounts hold exactly the authorised payments;
 * - its clearing accounts hold exactly the payouts the bank has, or is being sent, and hasn't settled;
 * - each bank's settlement account holds what has settled through it;
 * - what the bank has received is exactly the payouts it accepted, once each, with the same amounts,
 *   settled or returned as we have them;
 * - each payment's refunded amount is the sum of its refunds (in progress or done);
 * - each payment's, refund's and payout's version and state are its last event's, in the event log.
 *
 * The ledger, the bank and this database can't be read at one instant, so the checks that compare them
 * run only when nothing is in progress (`quiet`); the rest always run.
 */
class Reconcile(
    private val db: JdbcClient,
    private val tx: TransactionTemplate,
    private val ledger: LedgerOps,
    private val accounts: SystemAccounts,
    private val banks: Map<String, BankClient>,
) {
    fun run(): ReconcileReport {
        val read = TransactionTemplate(tx.transactionManager!!).apply {
            isReadOnly = true
            isolationLevel = Isolation.REPEATABLE_READ.value()
        }
        return read.execute { check() }
    }

    private fun check(): ReconcileReport {
        val inProgress = linkedMapOf(
            "payments" to count("SELECT count(*) FROM payments WHERE state IN ('authorising', 'capturing', 'voiding')"),
            "refunds" to count("SELECT count(*) FROM refunds WHERE state = 'refunding'"),
            "payouts" to count("SELECT count(*) FROM payouts WHERE state IN ('reserving', 'submitting', 'submitted', 'settling', 'returning')"),
        )
        val stuck = count(
            """SELECT (SELECT count(*) FROM payments WHERE state IN ('authorising', 'capturing', 'voiding') AND next_attempt_at IS NULL)
                    + (SELECT count(*) FROM refunds WHERE state = 'refunding' AND next_attempt_at IS NULL)
                    + (SELECT count(*) FROM payouts WHERE state IN ('reserving', 'submitting', 'submitted', 'settling', 'returning') AND next_attempt_at IS NULL)""",
        )
        // Each row's version is the last event of its stream in the event log, and its state is that event's.
        val eventLog = listOf("payments" to "payment", "refunds" to "refund", "payouts" to "payout").sumOf { (table, entity) ->
            count(
                """SELECT count(*) FROM $table r FULL JOIN (
                     SELECT DISTINCT ON (stream) substr(stream, ${entity.length + 2})::uuid AS id, version, type
                     FROM events WHERE stream LIKE '$entity-%' ORDER BY stream, version DESC) e USING (id)
                   WHERE r.id IS NULL OR e.id IS NULL OR r.version <> e.version OR e.type <> '$entity.' || r.state""",
            )
        }
        val violations = linkedMapOf(
            "stuck" to stuck,
            "event_log_not_matching_state" to eventLog,
            "refunds_not_summing" to count(
                """SELECT count(*) FROM payments p WHERE p.refunded <>
                     (SELECT coalesce(sum(r.amount), 0) FROM refunds r WHERE r.payment_id = p.id AND r.state IN ('refunding', 'refunded'))""",
            ),
        )
        val quiet = inProgress.values.all { it == 0L }
        if (quiet) {
            var holds = 0L
            var clearing = 0L
            var settlement = 0L
            val currencies = db.sql("SELECT currency FROM payments UNION SELECT currency FROM payouts").query(String::class.java).list().filterNotNull()
            for (c in currencies) {
                val holdsAccounts = (0 until accounts.shards).map { accounts.holds(c, it) } + accounts.legacyHolds(c)
                val authorised = sum("SELECT coalesce(sum(amount), 0) FROM payments WHERE currency = :c AND state = 'authorised'", c)
                if (holdsAccounts.sumOf(::balance) != authorised) holds++
                val clearingAccounts = (0 until accounts.shards).map { accounts.clearing(c, it) } + accounts.legacyClearing(c)
                val sent = sum("SELECT coalesce(sum(amount), 0) FROM payouts WHERE currency = :c AND state = 'submitted'", c)
                if (clearingAccounts.sumOf(::balance) != sent) clearing++
                for (bank in banks.keys) {
                    val settled = db.sql("SELECT coalesce(sum(amount), 0) FROM payouts WHERE currency = :c AND bank = :b AND state = 'settled'")
                        .param("c", c).param("b", bank).query(Long::class.java).single()
                    if (balance(accounts.settlement(bank, c)) != settled) settlement++
                }
            }
            violations["holds_not_matching_authorised"] = holds
            violations["clearing_not_matching_sent"] = clearing
            violations["settlement_not_matching_settled"] = settlement
            violations["bank_records_not_matching"] = banks.entries.sumOf { (name, bank) -> bankMismatches(name, bank) }
        }
        val totals = linkedMapOf(
            "payments" to count("SELECT count(*) FROM payments"),
            "refunds" to count("SELECT count(*) FROM refunds"),
            "payouts" to count("SELECT count(*) FROM payouts"),
        )
        return ReconcileReport(quiet && violations.values.all { it == 0L }, quiet, inProgress, violations, totals)
    }

    /** Ours and the bank's, side by side: each payout the bank accepted, once, in the same state. */
    private fun bankMismatches(name: String, bank: BankClient): Long {
        val theirs = bank.received().associateBy { UUID.fromString(it["id"].asString()) }
        val ours = db.sql("SELECT id, amount, state, accepted_by_bank, failure_reason FROM payouts WHERE bank = :b")
            .param("b", name).query { rs, _ ->
                OurPayout(rs.getObject(1, UUID::class.java), rs.getLong(2), rs.getString(3), rs.getBoolean(4), rs.getString(5))
            }.list().associateBy { it.id }
        var wrong = theirs.keys.count { it !in ours }.toLong() // the bank has a payout we never made
        for (p in ours.values) {
            val t = theirs[p.id]
            val expected = when {
                p.state == "settled" -> "settled"
                p.state == "returned" && p.failureReason?.startsWith("bank_returned") == true -> "returned"
                p.state == "submitted" -> "pending"
                else -> null // never accepted by the bank
            }
            val ok = if (expected == null) t == null && !p.acceptedByBank else t != null && t["status"].asString() == expected && t["amount"].asLong() == p.amount
            if (!ok) wrong++
        }
        return wrong
    }

    private data class OurPayout(val id: UUID, val amount: Long, val state: String, val acceptedByBank: Boolean, val failureReason: String?)

    /** A read, so safe to retry: a few times, before the whole check gives up (503). */
    private fun balance(account: UUID): Long {
        repeat(7) {
            try {
                return ledger.account(account)!!.balance
            } catch (_: LedgerUnavailable) {
                Thread.sleep(20)
            }
        }
        return ledger.account(account)!!.balance
    }

    private fun count(sql: String): Long = db.sql(sql).query(Long::class.java).single()

    private fun sum(sql: String, currency: String): Long = db.sql(sql).param("c", currency).query(Long::class.java).single()
}
