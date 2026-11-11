package dev.ledgerline.payments

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
import java.util.UUID

private const val KEY = "Idempotency-Key"

/**
 * The payments API over HTTP. Every write needs an Idempotency-Key. A write records its intent and then
 * takes the step at once: the answer is the resource when it's done (201 or 200), or 202 while it's still
 * in progress (the ledger was slow, say), in which case the worker finishes it: GET it for its state.
 * (Merchants use the gRPC API, MerchantApi, which makes the same writes.)
 */
@RestController
@RequestMapping("/v1")
class Api(private val payments: Payments, private val payouts: Payouts, private val writes: Writes) {

    @PostMapping("/payments")
    fun authorise(@RequestHeader(KEY, required = false) key: String?, @RequestBody body: AuthoriseRequest) = reply(writes.authorise(key, body))

    @GetMapping("/payments/{id}")
    fun payment(@PathVariable id: UUID) = payments.payment(id)

    @PostMapping("/payments/{id}/capture")
    fun capture(@RequestHeader(KEY, required = false) key: String?, @PathVariable id: UUID, @RequestBody(required = false) body: CaptureRequest?) =
        reply(writes.capture(key, id, body ?: CaptureRequest()))

    @PostMapping("/payments/{id}/void")
    fun void(@RequestHeader(KEY, required = false) key: String?, @PathVariable id: UUID) = reply(writes.void(key, id))

    @PostMapping("/payments/{id}/refunds")
    fun refund(@RequestHeader(KEY, required = false) key: String?, @PathVariable id: UUID, @RequestBody body: RefundRequest) =
        reply(writes.refund(key, id, body))

    @GetMapping("/payments/{id}/refunds")
    fun refunds(@PathVariable id: UUID) = payments.refundsOf(id)

    @GetMapping("/refunds/{id}")
    fun refundById(@PathVariable id: UUID) = payments.refund(id)

    @PostMapping("/payouts")
    fun payout(@RequestHeader(KEY, required = false) key: String?, @RequestBody body: PayoutRequest) = reply(writes.payout(key, body))

    @GetMapping("/payouts/{id}")
    fun payoutById(@PathVariable id: UUID) = payouts.payout(id)

    private fun reply(a: Answer): ResponseEntity<String> = ResponseEntity.status(a.status)
        .contentType(if (a.status >= 400) MediaType.APPLICATION_PROBLEM_JSON else MediaType.APPLICATION_JSON)
        .header("Idempotent-Replayed", a.replayed.toString())
        .body(a.body)
}

/** Internal: not routed from outside. */
@RestController
@RequestMapping("/internal")
class Internal(private val reconcile: Reconcile, private val merchants: Merchants, private val webhooks: Webhooks) {
    @GetMapping("/reconcile")
    fun reconcile(): ResponseEntity<ReconcileReport> = reconcile.run().let {
        ResponseEntity.status(if (it.ok) HttpStatus.OK else HttpStatus.CONFLICT).body(it)
    }

    /** A new merchant: its ledger account, API key and webhook secret (shown this once). */
    @PostMapping("/merchants")
    fun merchant(@RequestBody body: MerchantRequest): ResponseEntity<NewMerchant> = ResponseEntity.status(201).body(merchants.create(body))

    /** A merchant's failed webhook deliveries, queued again. */
    @PostMapping("/merchants/{id}/webhooks/redeliver")
    fun redeliver(@PathVariable id: UUID) = mapOf("queued" to webhooks.redeliver(merchants.byId(id).id))
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
