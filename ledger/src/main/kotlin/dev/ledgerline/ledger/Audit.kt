package dev.ledgerline.ledger

import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional

data class AuditReport(
    val ok: Boolean,
    /** For each check, how many things are wrong (0 when it holds). */
    val violations: Map<String, Long>,
    val totals: Map<String, Long>,
    val sumsByCurrency: Map<String, Long>,
)

/**
 * Checks the books from the event log, the source of truth, against the projections:
 *
 * - every currency sums to zero over all accounts (money is moved, never made or lost);
 * - every transaction's entries sum to zero per currency;
 * - every account's balance and version are what replaying its events gives;
 * - every entry matches its account event, and every account event has its entry;
 * - no account that may not go negative is below zero.
 *
 * It reads one consistent snapshot (repeatable read), so it can run while money moves.
 */
@Component
class Audit(private val db: JdbcClient) {

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    fun run(): AuditReport {
        val sums = db.sql("SELECT a.currency, sum(b.balance) FROM balances b JOIN accounts a ON a.id = b.account_id GROUP BY 1 ORDER BY 1")
            .query { rs, _ -> rs.getString(1) to rs.getLong(2) }.list().toMap()
        val violations = linkedMapOf(
            "currencies_not_summing_to_zero" to sums.values.count { it != 0L }.toLong(),
            "unbalanced_transactions" to count(
                """SELECT count(*) FROM (
                     SELECT e.stream FROM events e, jsonb_array_elements(e.data->'entries') x
                     WHERE e.type = 'TransactionRecorded'
                     GROUP BY e.stream, x->>'currency' HAVING sum((x->>'amount')::bigint) <> 0) u""",
            ),
            "balances_not_matching_events" to count(
                """WITH replay AS (
                     SELECT substr(stream, 9)::uuid AS account_id, max(version) AS version,
                            coalesce(sum((data->>'amount')::bigint) FILTER (WHERE type = 'Posted'), 0) AS balance
                     FROM events WHERE stream LIKE 'account-%' GROUP BY 1)
                   SELECT count(*) FROM balances b FULL JOIN replay r USING (account_id)
                   WHERE r.account_id IS NULL OR b.account_id IS NULL
                      OR b.balance <> r.balance OR b.version <> r.version""",
            ),
            "entries_not_matching_events" to count(
                """WITH posted AS (
                     SELECT substr(stream, 9)::uuid AS account_id, version, (data->>'transaction')::uuid AS transaction_id,
                            (data->>'amount')::bigint AS amount, (data->>'balanceAfter')::bigint AS balance_after
                     FROM events WHERE type = 'Posted')
                   SELECT count(*) FROM entries e FULL JOIN posted p
                     ON p.account_id = e.account_id AND p.version = e.account_version
                   WHERE p.account_id IS NULL OR e.account_id IS NULL OR p.transaction_id <> e.transaction_id
                      OR p.amount <> e.amount OR p.balance_after <> e.balance_after""",
            ),
            "transactions_not_matching_events" to count(
                """SELECT count(*) FROM transactions t FULL JOIN
                     (SELECT substr(stream, 13)::uuid AS id FROM events WHERE type = 'TransactionRecorded') r USING (id)
                   WHERE t.id IS NULL OR r.id IS NULL""",
            ),
            "overdrawn_accounts" to count("SELECT count(*) FROM balances WHERE balance < 0 AND NOT allow_negative"),
        )
        val totals = linkedMapOf(
            "accounts" to count("SELECT count(*) FROM accounts"),
            "transactions" to count("SELECT count(*) FROM transactions"),
            "entries" to count("SELECT count(*) FROM entries"),
            "events" to count("SELECT count(*) FROM events"),
        )
        return AuditReport(violations.values.all { it == 0L }, violations, totals, sums)
    }

    private fun count(sql: String): Long = db.sql(sql).query(Long::class.java).single()
}
