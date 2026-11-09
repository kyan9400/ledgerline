package dev.ledgerline.payments

import java.time.Instant
import java.util.UUID

/** Amounts are whole minor units (kopecks, cents) in a Long, as in the ledger. */
data class Payment(
    val id: UUID,
    val state: String,
    val customer: UUID,
    val merchant: UUID,
    val currency: String,
    val amount: Long,
    val captured: Long,
    val refunded: Long,
    val declineReason: String?,
    val voidReason: String?,
    val description: String,
    val expiresAt: Instant?,
    val createdAt: Instant,
    val updatedAt: Instant,
    val version: Int,
    val lastError: String?,
    /** Which of the holds accounts this payment's money waits in (null: the original one). */
    val holdsAccount: UUID?,
)

data class Refund(
    val id: UUID,
    val payment: UUID,
    val state: String,
    val amount: Long,
    val failureReason: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
    val version: Int,
    val lastError: String?,
)

data class Payout(
    val id: UUID,
    val state: String,
    val account: UUID,
    val currency: String,
    val amount: Long,
    val bank: String,
    val beneficiary: String,
    val acceptedByBank: Boolean,
    val failureReason: String?,
    val description: String,
    val createdAt: Instant,
    val updatedAt: Instant,
    val version: Int,
    val lastError: String?,
    /** Which of the clearing accounts this payout's money waits in (null: the original one). */
    val clearingAccount: UUID?,
)

/** Authorise: hold `amount` of the customer's money for the merchant. */
data class AuthoriseRequest(val customer: UUID, val merchant: UUID, val amount: Long, val description: String = "")

/** Capture all of the hold, or less: the rest goes back to the customer. Once. */
data class CaptureRequest(val amount: Long? = null)

data class RefundRequest(val amount: Long)

data class PayoutRequest(
    val account: UUID,
    val amount: Long,
    val bank: String,
    val beneficiary: String,
    val description: String = "",
)

val PAYMENT_STEPS = setOf("authorising", "capturing", "voiding")
val PAYOUT_STEPS = setOf("reserving", "submitting", "submitted", "settling", "returning")

/** A refusal the client can act on, with an HTTP status and a stable code. */
open class PaymentsException(
    val status: Int,
    val code: String,
    message: String,
    val details: Map<String, Any?> = emptyMap(),
) : RuntimeException(message)

class NotFound(what: String) : PaymentsException(404, "not_found", "$what not found")

class Invalid(message: String) : PaymentsException(422, "invalid_request", message)

class WrongState(what: String, state: String, wanted: String) :
    PaymentsException(409, "invalid_state", "$what is $state; this needs it $wanted", mapOf("state" to state))

class LedgerUnavailable(reason: String) :
    PaymentsException(503, "ledger_unavailable", "the ledger can't be reached ($reason): retry with the same Idempotency-Key")

/** RFC 9457 problem details. */
fun problem(e: PaymentsException): Map<String, Any?> = linkedMapOf(
    "type" to "https://github.com/kyan9400/ledgerline#${e.code}",
    "title" to e.code.replace('_', ' ').replaceFirstChar { it.uppercase() },
    "status" to e.status,
    "code" to e.code,
    "detail" to e.message,
) + e.details
