package dev.ledgerline.ledger

import java.time.Instant
import java.util.UUID

/** Amounts are whole minor units (kopecks, cents) in a Long: never floating point. */
data class Account(
    val id: UUID,
    val code: String,
    val currency: String,
    val kind: String,
    val allowNegative: Boolean,
    val balance: Long,
    val version: Int,
    val openedAt: Instant,
)

data class OpenAccount(
    val code: String,
    val currency: String,
    val kind: String,
    val allowNegative: Boolean = false,
)

data class EntryRequest(val account: UUID, val amount: Long)

/** Any balanced set of entries: per currency, the amounts sum to zero. */
data class PostingRequest(
    val entries: List<EntryRequest>,
    val description: String = "",
    val metadata: Map<String, String> = emptyMap(),
)

/** Money from one account to another of the same currency. */
data class TransferRequest(
    val from: UUID,
    val to: UUID,
    val amount: Long,
    val description: String = "",
    val metadata: Map<String, String> = emptyMap(),
)

data class TransactionEntry(
    val account: UUID,
    val currency: String,
    val amount: Long,
    val balanceAfter: Long,
)

data class Transaction(
    val id: UUID,
    val kind: String,
    val reverses: UUID?,
    val description: String,
    val metadata: Map<String, String>,
    val recordedAt: Instant,
    val entries: List<TransactionEntry>,
)

data class StatementEntry(
    val transaction: UUID,
    val amount: Long,
    val balanceAfter: Long,
    val version: Int,
    val recordedAt: Instant,
)

data class Statement(val account: UUID, val entries: List<StatementEntry>, val next: Int?)

/** A refusal the client can act on, with an HTTP status and a stable code. */
open class LedgerException(
    val status: Int,
    val code: String,
    message: String,
    val details: Map<String, Any?> = emptyMap(),
) : RuntimeException(message)

class NotFound(what: String) : LedgerException(404, "not_found", "$what not found")

class Invalid(message: String, details: Map<String, Any?> = emptyMap()) :
    LedgerException(422, "invalid_request", message, details)

class InsufficientFunds(account: UUID, balance: Long, change: Long) :
    LedgerException(
        409,
        "insufficient_funds",
        "account $account has $balance and can't go below zero (change: $change)",
        mapOf("account" to account, "balance" to balance, "change" to change),
    )

class Conflict(code: String, message: String) : LedgerException(409, code, message)
