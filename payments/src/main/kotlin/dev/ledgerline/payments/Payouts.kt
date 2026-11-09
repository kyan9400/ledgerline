package dev.ledgerline.payments

import dev.ledgerline.platform.Outbox
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.json.JsonMapper
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.random.Random

/**
 * Payouts to accounts at other banks, as a saga:
 *
 * ```
 * reserving  -> submitting   customer -> clearing           (or refused: not enough money)
 * submitting -> submitted    sent to the bank, idempotent by payout id (or returning: the bank refused)
 * submitted  -> settling     the bank says settled          (or returning: the bank returned it)
 * settling   -> settled      clearing -> settlement
 * returning  -> returned     clearing -> customer: the money comes back
 * ```
 *
 * The step that can't be undone is the bank's. A timeout there says nothing about whether the bank got
 * the payout, so the money stays reserved and the payout is sent again, with the same id, until the bank
 * answers; it is never given back on a guess. Every ledger step has a key fixed by the payout and the step.
 */
class Payouts(
    private val db: JdbcClient,
    private val tx: TransactionTemplate,
    private val ledger: LedgerOps,
    private val accounts: SystemAccounts,
    private val banks: Map<String, BankClient>,
    private val clock: Clock,
    outbox: Outbox,
    json: JsonMapper,
    meters: MeterRegistry,
) {
    val payouts = Machine(db, outbox, json, clock, meters, "payouts", "payout", PAYOUT_STEPS) { payout(it) }

    fun createIn(req: PayoutRequest): Payout {
        if (req.amount <= 0) throw Invalid("amount must be positive")
        if (req.bank !in banks) throw Invalid("unknown bank ${req.bank} (known: ${banks.keys.sorted().joinToString()})")
        if (!Regex("^[A-Za-z0-9-]{1,34}$").matches(req.beneficiary)) throw Invalid("beneficiary is an account number: 1-34 letters, digits and -")
        if (req.description.length > 500) throw Invalid("description is at most 500 characters")
        val account = ledger.facts(req.account) ?: throw Invalid("no account ${req.account}")
        if (account.kind != "customer") throw Invalid("${req.account} is not a customer account")
        val clearing = accounts.clearing(account.currency, Random.nextInt(accounts.shards))
        accounts.settlement(req.bank, account.currency)
        val id = UUID.randomUUID()
        val now = Timestamp.from(clock.instant())
        db.sql(
            """INSERT INTO payouts (id, account, currency, amount, bank, beneficiary, state, description, version, next_attempt_at, created_at, updated_at, clearing_account)
               VALUES (:id, :a, :cur, :amt, :bank, :b, 'reserving', :d, 1, :next, :now, :now, :clearing)""",
        ).param("id", id).param("a", req.account).param("cur", account.currency).param("amt", req.amount).param("bank", req.bank)
            .param("clearing", clearing)
            .param("b", req.beneficiary).param("d", req.description).param("next", Timestamp.from(clock.instant().plus(Machine.LEASE)))
            .param("now", now).update()
        payouts.created(id, "reserving")
        return payout(id)
    }

    /** One step of the saga, if it has one to take. Returns the payout as it is then. */
    fun drive(id: UUID): Payout {
        val p = payout(id)
        if (p.state !in PAYOUT_STEPS) return p
        val meta = mapOf("payout" to id.toString())
        val bank = banks.getValue(p.bank)
        val clearing = p.clearingAccount ?: accounts.legacyClearing(p.currency)
        val attempts = db.sql("SELECT attempts FROM payouts WHERE id = :id").param("id", id).query(Int::class.java).single()
        val step = when (p.state) {
            "reserving" -> when (val o = ledger.transfer(key(id, "reserve"), p.account, clearing, p.amount, "payout reserved", meta)) {
                is Outcome.Done -> Step.To("submitting")
                is Outcome.Refused -> Step.To("refused", mapOf("failure_reason" to o.code))
                is Outcome.Unavailable -> Step.Retry(o.reason)
            }
            "submitting" -> when (val s = bank.submit(id, p.amount, p.currency, p.beneficiary)) {
                BankClient.Submitted.Accepted -> Step.To("submitted", mapOf("accepted_by_bank" to true))
                is BankClient.Submitted.Rejected -> Step.To("returning", mapOf("failure_reason" to "bank_rejected:${s.code}"))
                is BankClient.Submitted.Unknown -> Step.Retry("bank: ${s.reason}")
            }
            "submitted" -> when (val s = bank.status(id)) {
                is BankClient.Status.Known -> when (s.status) {
                    "settled" -> Step.To("settling")
                    "returned" -> Step.To("returning", mapOf("failure_reason" to "bank_returned:${s.returnReason ?: "unknown"}"))
                    else -> Step.Later(poll(attempts))
                }
                // The bank has no record of it (it lost it): send it again, by the same id.
                BankClient.Status.Missing -> Step.To("submitting")
                is BankClient.Status.Unknown -> Step.Retry("bank: ${s.reason}")
            }
            "settling" -> when (val o = ledger.transfer(key(id, "settle"), clearing, accounts.settlement(p.bank, p.currency), p.amount, "payout settled", meta)) {
                is Outcome.Done -> Step.To("settled")
                is Outcome.Refused -> Step.Stuck("ledger refused the settlement: ${o.code} ${o.detail}")
                is Outcome.Unavailable -> Step.Retry(o.reason)
            }
            else -> when (val o = ledger.transfer(key(id, "return"), clearing, p.account, p.amount, "payout returned", meta)) {
                is Outcome.Done -> Step.To("returned")
                is Outcome.Refused -> Step.Stuck("ledger refused the return: ${o.code} ${o.detail}")
                is Outcome.Unavailable -> Step.Retry(o.reason)
            }
        }
        tx.executeWithoutResult { applyStep(payouts, id, p.version, attempts, step) }
        return payout(id)
    }

    fun payout(id: UUID): Payout = db.sql("SELECT * FROM payouts WHERE id = :id").param("id", id).query { rs, _ -> payoutOf(rs) }
        .optional().orElseThrow { NotFound("payout $id") }

    private fun key(id: UUID, step: String) = "payments:payout:$id:$step"

    /** How long to wait before asking the bank again: 250 ms, doubling, at most 10 s. */
    private fun poll(attempts: Int): Duration = Duration.ofMillis((250L shl attempts.coerceIn(0, 6)).coerceAtMost(10_000))

    private fun payoutOf(rs: ResultSet) = Payout(
        rs.getObject("id", UUID::class.java), rs.getString("state"), rs.getObject("account", UUID::class.java), rs.getString("currency"),
        rs.getLong("amount"), rs.getString("bank"), rs.getString("beneficiary"), rs.getBoolean("accepted_by_bank"),
        rs.getString("failure_reason"), rs.getString("description"), rs.getTimestamp("created_at").toInstant(),
        rs.getTimestamp("updated_at").toInstant(), rs.getInt("version"), rs.getString("last_error"),
        rs.getObject("clearing_account", UUID::class.java),
    )
}
