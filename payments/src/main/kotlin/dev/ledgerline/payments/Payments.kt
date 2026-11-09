package dev.ledgerline.payments

import dev.ledgerline.platform.Outbox
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.transaction.support.TransactionTemplate
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.random.Random

/**
 * Card payments, as a state machine over ledger postings:
 *
 * ```
 * authorising -> authorised   customer -> holds              (or declined: not enough money)
 * authorised  -> capturing -> captured   holds -> merchant, the rest of the hold -> customer
 * authorised  -> voiding   -> voided     holds -> customer   (on request, or when the hold expires)
 * captured: refunds, each   refunding -> refunded   merchant -> customer   (or failed)
 * ```
 *
 * Each arrow out of an -ing state is one ledger posting, with an idempotency key fixed by the payment and
 * the step. The API records the intent, then drives the step at once; if it can't finish (the ledger is
 * slow, the service dies), the worker drives it later, and the ledger's idempotency makes the second try
 * land on the first's result.
 */
class Payments(
    private val db: JdbcClient,
    private val tx: TransactionTemplate,
    private val ledger: LedgerOps,
    private val accounts: SystemAccounts,
    private val clock: Clock,
    private val authorisationTtl: Duration,
    outbox: Outbox,
    meters: MeterRegistry,
) {
    val payments = Machine(db, outbox, clock, meters, "payments", "payment", PAYMENT_STEPS) { payment(it) }
    val refunds = Machine(db, outbox, clock, meters, "refunds", "refund", setOf("refunding")) { refund(it) }

    // --- intents (in the idempotency layer's transaction) --------------------------------------------

    fun createIn(req: AuthoriseRequest): Payment {
        if (req.amount <= 0) throw Invalid("amount must be positive")
        if (req.description.length > 500) throw Invalid("description is at most 500 characters")
        val customer = ledger.facts(req.customer) ?: throw Invalid("no account ${req.customer}")
        val merchant = ledger.facts(req.merchant) ?: throw Invalid("no account ${req.merchant}")
        if (customer.kind != "customer") throw Invalid("${req.customer} is not a customer account")
        if (merchant.kind != "merchant") throw Invalid("${req.merchant} is not a merchant account")
        if (customer.currency != merchant.currency) throw Invalid("the customer's and the merchant's currencies differ")
        val holds = accounts.holds(customer.currency, Random.nextInt(accounts.shards))
        val id = UUID.randomUUID()
        val now = Timestamp.from(clock.instant())
        db.sql(
            """INSERT INTO payments (id, customer, merchant, currency, amount, state, description, version, next_attempt_at, created_at, updated_at, holds_account)
               VALUES (:id, :c, :m, :cur, :a, 'authorising', :d, 1, :next, :now, :now, :holds)""",
        ).param("id", id).param("c", req.customer).param("m", req.merchant).param("cur", customer.currency).param("a", req.amount)
            .param("holds", holds)
            .param("d", req.description).param("next", Timestamp.from(clock.instant().plus(Machine.LEASE))).param("now", now).update()
        payments.created(id, "authorising")
        return payment(id)
    }

    fun captureIn(id: UUID, req: CaptureRequest): Payment {
        val p = lock(id)
        if (p.state != "authorised") throw WrongState("payment $id", p.state, "authorised")
        val amount = req.amount ?: p.amount
        if (amount !in 1..p.amount) throw Invalid("a capture is from 1 to the authorised ${p.amount}")
        payments.change(id, p.version, "capturing", mapOf("capture_amount" to amount), lease = true)
        return payment(id)
    }

    fun voidIn(id: UUID): Payment {
        val p = lock(id)
        if (p.state != "authorised") throw WrongState("payment $id", p.state, "authorised")
        payments.change(id, p.version, "voiding", mapOf("void_reason" to "requested"), lease = true)
        return payment(id)
    }

    fun refundIn(paymentId: UUID, req: RefundRequest): Refund {
        val p = lock(paymentId)
        if (p.state != "captured") throw WrongState("payment $paymentId", p.state, "captured")
        if (req.amount <= 0) throw Invalid("amount must be positive")
        if (p.refunded + req.amount > p.captured) {
            throw PaymentsException(
                409, "refund_exceeds_capture",
                "payment $paymentId has ${p.captured - p.refunded} left to refund", mapOf("refundable" to p.captured - p.refunded),
            )
        }
        db.sql("UPDATE payments SET refunded = refunded + :a WHERE id = :id").param("a", req.amount).param("id", paymentId).update()
        val id = UUID.randomUUID()
        val now = Timestamp.from(clock.instant())
        db.sql(
            """INSERT INTO refunds (id, payment_id, amount, state, version, next_attempt_at, created_at, updated_at)
               VALUES (:id, :p, :a, 'refunding', 1, :next, :now, :now)""",
        ).param("id", id).param("p", paymentId).param("a", req.amount)
            .param("next", Timestamp.from(clock.instant().plus(Machine.LEASE))).param("now", now).update()
        refunds.created(id, "refunding")
        return refund(id)
    }

    /** Hold expired: void it. Conditional on the version, so a capture that got there first wins. */
    fun expire(limit: Int): Int {
        val due = db.sql("SELECT id, version FROM payments WHERE state = 'authorised' AND expires_at <= :now LIMIT :n")
            .param("now", Timestamp.from(clock.instant())).param("n", limit)
            .query { rs, _ -> rs.getObject(1, UUID::class.java) to rs.getInt(2) }.list()
        return due.count { (id, version) -> tx.execute { payments.change(id, version, "voiding", mapOf("void_reason" to "expired")) } }
    }

    // --- steps ---------------------------------------------------------------------------------------

    /** Drives a payment's step, if it has one. Returns the payment as it is then. */
    fun drivePayment(id: UUID): Payment {
        val p = payment(id)
        if (p.state !in PAYMENT_STEPS) return p
        val meta = mapOf("payment" to id.toString())
        val holds = p.holdsAccount ?: accounts.legacyHolds(p.currency)
        val step = when (p.state) {
            "authorising" -> when (val o = ledger.transfer(key(id, "authorise"), p.customer, holds, p.amount, "card authorisation", meta)) {
                is Outcome.Done -> Step.To("authorised", mapOf("expires_at" to clock.instant().plus(authorisationTtl)))
                is Outcome.Refused -> Step.To("declined", mapOf("decline_reason" to o.code))
                is Outcome.Unavailable -> Step.Retry(o.reason)
            }
            "capturing" -> {
                val amount = captureAmount(id)
                val entries = listOfNotNull(
                    holds to -p.amount,
                    p.merchant to amount,
                    (p.customer to p.amount - amount).takeIf { amount < p.amount },
                )
                when (val o = ledger.post(key(id, "capture"), entries, "card capture", meta)) {
                    is Outcome.Done -> Step.To("captured", mapOf("captured" to amount, "capture_amount" to null))
                    is Outcome.Refused -> Step.Stuck("ledger refused the capture: ${o.code} ${o.detail}")
                    is Outcome.Unavailable -> Step.Retry(o.reason)
                }
            }
            else -> when (val o = ledger.transfer(key(id, "void"), holds, p.customer, p.amount, "card authorisation voided", meta)) {
                is Outcome.Done -> Step.To("voided")
                is Outcome.Refused -> Step.Stuck("ledger refused the void: ${o.code} ${o.detail}")
                is Outcome.Unavailable -> Step.Retry(o.reason)
            }
        }
        tx.executeWithoutResult { applyStep(payments, id, p.version, attemptsOf("payments", id), step) }
        return payment(id)
    }

    fun driveRefund(id: UUID): Refund {
        val r = refund(id)
        if (r.state != "refunding") return r
        val p = payment(r.payment)
        val o = ledger.transfer("payments:refund:$id", p.merchant, p.customer, r.amount, "card refund", mapOf("payment" to p.id.toString(), "refund" to id.toString()))
        tx.executeWithoutResult {
            when (o) {
                is Outcome.Done -> refunds.change(id, r.version, "refunded")
                is Outcome.Refused -> {
                    // The merchant hasn't the money (or the ledger refused for another reason): the amount
                    // can be refunded again later.
                    if (refunds.change(id, r.version, "failed", mapOf("failure_reason" to o.code))) {
                        db.sql("UPDATE payments SET refunded = refunded - :a WHERE id = :id").param("a", r.amount).param("id", p.id).update()
                    }
                }
                is Outcome.Unavailable -> refunds.retry(id, r.version, attemptsOf("refunds", id), o.reason)
            }
        }
        return refund(id)
    }

    // --- reading -------------------------------------------------------------------------------------

    fun payment(id: UUID): Payment = db.sql("SELECT * FROM payments WHERE id = :id").param("id", id).query { rs, _ -> paymentOf(rs) }
        .optional().orElseThrow { NotFound("payment $id") }

    fun refund(id: UUID): Refund = db.sql("SELECT * FROM refunds WHERE id = :id").param("id", id).query { rs, _ -> refundOf(rs) }
        .optional().orElseThrow { NotFound("refund $id") }

    fun refundsOf(paymentId: UUID): List<Refund> = payment(paymentId).let {
        db.sql("SELECT * FROM refunds WHERE payment_id = :id ORDER BY created_at").param("id", paymentId).query { rs, _ -> refundOf(rs) }.list()
    }

    private fun lock(id: UUID): Payment = db.sql("SELECT * FROM payments WHERE id = :id FOR UPDATE").param("id", id)
        .query { rs, _ -> paymentOf(rs) }.optional().orElseThrow { NotFound("payment $id") }

    private fun captureAmount(id: UUID): Long =
        db.sql("SELECT capture_amount FROM payments WHERE id = :id").param("id", id).query(Long::class.java).single()

    private fun attemptsOf(table: String, id: UUID): Int =
        db.sql("SELECT attempts FROM $table WHERE id = :id").param("id", id).query(Int::class.java).single()

    private fun key(id: UUID, step: String) = "payments:payment:$id:$step"

    private fun paymentOf(rs: ResultSet) = Payment(
        rs.getObject("id", UUID::class.java), rs.getString("state"), rs.getObject("customer", UUID::class.java),
        rs.getObject("merchant", UUID::class.java), rs.getString("currency"), rs.getLong("amount"), rs.getLong("captured"),
        rs.getLong("refunded"), rs.getString("decline_reason"), rs.getString("void_reason"), rs.getString("description"),
        rs.getTimestamp("expires_at")?.toInstant(), rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant(),
        rs.getInt("version"), rs.getString("last_error"), rs.getObject("holds_account", UUID::class.java),
    )

    private fun refundOf(rs: ResultSet) = Refund(
        rs.getObject("id", UUID::class.java), rs.getObject("payment_id", UUID::class.java), rs.getString("state"), rs.getLong("amount"),
        rs.getString("failure_reason"), rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant(),
        rs.getInt("version"), rs.getString("last_error"),
    )
}

/** Applies a step's outcome, in the caller's transaction. */
fun applyStep(machine: Machine, id: UUID, version: Int, attempts: Int, step: Step) {
    when (step) {
        is Step.To -> machine.change(id, version, step.state, step.set)
        is Step.Retry -> machine.retry(id, version, attempts, step.error)
        is Step.Later -> machine.later(id, version, step.delay)
        is Step.Stuck -> machine.stuck(id, version, step.error)
    }
}
