package dev.ledgerline.payments

import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RestControllerAdvice
import tools.jackson.databind.json.JsonMapper
import java.util.UUID

private const val KEY = "Idempotency-Key"

/**
 * The payments API. Every write needs an Idempotency-Key. A write records its intent and then takes the
 * step at once: the answer is the resource when it's done (201 or 200), or 202 while it's still in
 * progress (the ledger was slow, say), in which case the worker finishes it: GET it for its state.
 */
@RestController
@RequestMapping("/v1")
class Api(
    private val payments: Payments,
    private val payouts: Payouts,
    private val idem: Idempotency,
    private val json: JsonMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @PostMapping("/payments")
    fun authorise(@RequestHeader(KEY, required = false) key: String?, @RequestBody body: AuthoriseRequest) =
        write(key, "authorise", body, done = 201, intent = { payments.createIn(body) }, id = { (it as Payment).id }) { id ->
            payments.drivePayment(id).let { it to (it.state in PAYMENT_STEPS) }
        }

    @GetMapping("/payments/{id}")
    fun payment(@PathVariable id: UUID) = payments.payment(id)

    @PostMapping("/payments/{id}/capture")
    fun capture(@RequestHeader(KEY, required = false) key: String?, @PathVariable id: UUID, @RequestBody(required = false) body: CaptureRequest?) =
        (body ?: CaptureRequest()).let { b ->
            write(key, "capture $id", b, done = 200, intent = { payments.captureIn(id, b) }, id = { id }) {
                payments.drivePayment(it).let { p -> p to (p.state in PAYMENT_STEPS) }
            }
        }

    @PostMapping("/payments/{id}/void")
    fun void(@RequestHeader(KEY, required = false) key: String?, @PathVariable id: UUID) =
        write(key, "void $id", emptyMap<String, Any>(), done = 200, intent = { payments.voidIn(id) }, id = { id }) {
            payments.drivePayment(it).let { p -> p to (p.state in PAYMENT_STEPS) }
        }

    @PostMapping("/payments/{id}/refunds")
    fun refund(@RequestHeader(KEY, required = false) key: String?, @PathVariable id: UUID, @RequestBody body: RefundRequest) =
        write(key, "refund $id", body, done = 201, intent = { payments.refundIn(id, body) }, id = { (it as Refund).id }) { rid ->
            payments.driveRefund(rid).let { it to (it.state == "refunding") }
        }

    @GetMapping("/payments/{id}/refunds")
    fun refunds(@PathVariable id: UUID) = payments.refundsOf(id)

    @GetMapping("/refunds/{id}")
    fun refundById(@PathVariable id: UUID) = payments.refund(id)

    /** A payout's first step (reserving the money) is taken at once; the bank's part is the worker's. */
    @PostMapping("/payouts")
    fun payout(@RequestHeader(KEY, required = false) key: String?, @RequestBody body: PayoutRequest) =
        write(key, "payout", body, done = 202, intent = { payouts.createIn(body) }, id = { (it as Payout).id }) { pid ->
            payouts.drive(pid) to true
        }

    @GetMapping("/payouts/{id}")
    fun payoutById(@PathVariable id: UUID) = payouts.payout(id)

    /**
     * The intent, in the idempotency layer's transaction; then (only the first time) the step, and the
     * answer stored for retries. `step` returns the resource and whether it is still in progress.
     */
    private fun write(
        key: String?,
        operation: String,
        request: Any,
        done: Int,
        intent: () -> Any,
        id: (Any) -> UUID,
        step: (UUID) -> Pair<Any, Boolean>,
    ): ResponseEntity<String> {
        var resource: UUID? = null
        val first = idem.run(key, operation, request) { intent().let { resource = id(it); 202 to it } }
        if (first.replayed || first.status >= 400) return reply(first)
        val (now, inProgress) = try {
            step(resource!!)
        } catch (e: Exception) {
            // The intent is recorded and the worker will take the step: the answer is the 202 already stored.
            log.debug("{}: step deferred: {}", operation, e.toString())
            return reply(first)
        }
        val answer = Answer(if (inProgress) 202 else done, json.writeValueAsString(now), replayed = false)
        idem.update(key!!, answer.status, answer.body)
        return reply(answer)
    }

    private fun reply(a: Answer): ResponseEntity<String> = ResponseEntity.status(a.status)
        .contentType(if (a.status >= 400) MediaType.APPLICATION_PROBLEM_JSON else MediaType.APPLICATION_JSON)
        .header("Idempotent-Replayed", a.replayed.toString())
        .body(a.body)
}

/** Internal: not routed from outside. */
@RestController
@RequestMapping("/internal")
class Internal(private val reconcile: Reconcile) {
    @GetMapping("/reconcile")
    fun reconcile(): ResponseEntity<ReconcileReport> = reconcile.run().let {
        ResponseEntity.status(if (it.ok) HttpStatus.OK else HttpStatus.CONFLICT).body(it)
    }
}

@RestControllerAdvice
@Suppress("UNUSED_PARAMETER")
class Errors {
    @ExceptionHandler(PaymentsException::class)
    fun payments(e: PaymentsException): ResponseEntity<Map<String, Any?>> =
        ResponseEntity.status(e.status).contentType(MediaType.APPLICATION_PROBLEM_JSON)
            .apply { if (e.status == 503) header("Retry-After", "1") }
            .body(problem(e))

    @ExceptionHandler(HttpMessageNotReadableException::class)
    fun unreadable(e: HttpMessageNotReadableException): ResponseEntity<Map<String, Any?>> =
        payments(Invalid("the request body isn't valid JSON for this operation"))
}
