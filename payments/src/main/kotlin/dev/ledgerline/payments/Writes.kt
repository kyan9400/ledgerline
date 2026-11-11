package dev.ledgerline.payments

import org.slf4j.LoggerFactory
import tools.jackson.databind.json.JsonMapper
import java.util.UUID

/**
 * Every write, as both APIs (HTTP and the merchants' gRPC) make it: the intent in the idempotency
 * layer's transaction, then (only the first time) the step, and the answer stored for retries. The answer
 * is the resource when its step is done, or 202 while it is still in progress (the worker finishes it).
 */
class Writes(
    private val payments: Payments,
    private val payouts: Payouts,
    private val idem: Idempotency,
    private val json: JsonMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun authorise(key: String?, body: AuthoriseRequest): Answer =
        write(key, "authorise", body, done = 201, intent = { payments.createIn(body) }, id = { (it as Payment).id }) { id ->
            payments.drivePayment(id).let { it to (it.state in PAYMENT_STEPS) }
        }

    fun capture(key: String?, id: UUID, body: CaptureRequest): Answer =
        write(key, "capture $id", body, done = 200, intent = { payments.captureIn(id, body) }, id = { id }) {
            payments.drivePayment(it).let { p -> p to (p.state in PAYMENT_STEPS) }
        }

    fun void(key: String?, id: UUID): Answer =
        write(key, "void $id", emptyMap<String, Any>(), done = 200, intent = { payments.voidIn(id) }, id = { id }) {
            payments.drivePayment(it).let { p -> p to (p.state in PAYMENT_STEPS) }
        }

    fun refund(key: String?, id: UUID, body: RefundRequest): Answer =
        write(key, "refund $id", body, done = 201, intent = { payments.refundIn(id, body) }, id = { (it as Refund).id }) { rid ->
            payments.driveRefund(rid).let { it to (it.state == "refunding") }
        }

    /** A payout's first step (reserving the money) is taken at once; the bank's part is the worker's. */
    fun payout(key: String?, body: PayoutRequest): Answer =
        write(key, "payout", body, done = 202, intent = { payouts.createIn(body) }, id = { (it as Payout).id }) { pid ->
            payouts.drive(pid) to true
        }

    /** `step` returns the resource and whether it is still in progress. */
    private fun write(
        key: String?,
        operation: String,
        request: Any,
        done: Int,
        intent: () -> Any,
        id: (Any) -> UUID,
        step: (UUID) -> Pair<Any, Boolean>,
    ): Answer {
        var resource: UUID? = null
        val first = idem.run(key, operation, request) { intent().let { resource = id(it); 202 to it } }
        if (first.replayed || first.status >= 400) return first
        val (now, inProgress) = try {
            step(resource!!)
        } catch (e: Exception) {
            // The intent is recorded and the worker will take the step: the answer is the 202 already stored.
            log.debug("{}: step deferred: {}", operation, e.toString())
            return first
        }
        val answer = Answer(if (inProgress) 202 else done, json.writeValueAsString(now), replayed = false)
        idem.update(key!!, answer.status, answer.body)
        return answer
    }
}
