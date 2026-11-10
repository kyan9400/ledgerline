package dev.ledgerline.payments

import io.micrometer.core.instrument.MeterRegistry

/** The scorer's verdict on one payment. */
data class Verdict(val score: Double?, val decision: String, val model: String?, val reasons: String?)

/**
 * Asks the fraud scorer about a card payment before its money moves, once (the verdict is kept on the
 * payment, so a retried step doesn't ask again).
 *
 * - `off`: never asks.
 * - `shadow`: asks and records the verdict, but never declines: to measure a model before trusting it.
 * - `enforce`: a `decline` verdict declines the payment (decline_reason fraud_suspected).
 *
 * It fails open: if the scorer doesn't answer within its timeout (it is down, or still rebuilding its
 * features), the payment goes on, marked `unscored`. A fraud check that can stop all card payments is a
 * worse outage than the fraud it stops; `payments.fraud.unscored` counts how often it happens.
 */
class FraudCheck(private val http: HttpService?, val mode: String, meters: MeterRegistry) {
    private val unscored = meters.counter("payments.fraud.unscored")
    private val declined = meters.counter("payments.fraud.declined")

    init {
        require(mode in setOf("off", "shadow", "enforce")) { "payments.fraud.mode is off, shadow or enforce, not $mode" }
    }

    val on get() = mode != "off" && http != null

    fun verdict(p: Payment): Verdict {
        val o = http!!.post(
            "/v1/score", p.id.toString(),
            mapOf("payment" to p.id, "customer" to p.customer, "merchant" to p.merchant, "amount" to p.amount, "at" to p.createdAt.toString()),
        )
        if (o !is Outcome.Done) {
            unscored.increment()
            return Verdict(null, "unscored", null, (o as? Outcome.Unavailable)?.reason ?: (o as Outcome.Refused).code)
        }
        val b = o.body
        return Verdict(
            b["score"].asDouble(), b["decision"].asString(), b["model"]?.asString(),
            b["reasons"]?.let { r -> (0 until r.size()).joinToString(",") { r[it].asString() } },
        )
    }

    /** Whether this verdict stops the payment. */
    fun declines(v: Verdict): Boolean = (mode == "enforce" && v.decision == "decline").also { if (it) declined.increment() }
}
