package dev.ledgerline.ledger

import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.json.JsonMapper
import java.security.MessageDigest

/** An answer to a write: the status and the exact body, first time or replayed. */
data class Stored(val status: Int, val body: String, val replayed: Boolean)

/**
 * Runs a write at most once per idempotency key.
 *
 * The key is claimed with an INSERT in the same database transaction as the write. A second request
 * with the key waits on that insert until the first commits, then gets the stored answer: the same
 * status and body, a refusal included. If the first fails unexpectedly (and rolls back), the key is
 * free again. The same key with a different request is refused.
 */
@Component
class Idempotency(private val db: JdbcClient, private val json: JsonMapper, private val tx: TransactionTemplate) {

    fun run(key: String?, operation: String, request: Any, status: Int = 201, write: () -> Any): Stored {
        if (key.isNullOrBlank()) throw IdempotencyKeyMissing()
        if (key.length > 200) throw Invalid("Idempotency-Key is at most 200 characters")
        val hash = sha256("$operation\n${json.writeValueAsString(request)}")
        return tx.execute {
            val claimed = db.sql(
                """INSERT INTO idempotency_keys (key, request_hash, status, response)
                   VALUES (:k, :h, 0, '') ON CONFLICT (key) DO NOTHING""",
            ).param("k", key).param("h", hash).update() == 1
            if (!claimed) {
                val (storedHash, storedStatus, body) = db.sql("SELECT request_hash, status, response FROM idempotency_keys WHERE key = :k")
                    .param("k", key).query { rs, _ -> Triple(rs.getString(1), rs.getInt(2), rs.getString(3)) }.single()
                if (storedHash != hash) throw IdempotencyKeyReused()
                return@execute Stored(storedStatus, body, replayed = true)
            }
            val (code, body) = try {
                status to json.writeValueAsString(write())
            } catch (e: LedgerException) {
                // A refusal is an answer too: a retry gets the same one. (Refusals happen before any write.)
                e.status to json.writeValueAsString(problem(e))
            }
            db.sql("UPDATE idempotency_keys SET status = :s, response = :r WHERE key = :k")
                .param("s", code).param("r", body).param("k", key).update()
            Stored(code, body, replayed = false)
        }
    }

    private fun sha256(s: String): String =
        MessageDigest.getInstance("SHA-256").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }
}

class IdempotencyKeyMissing : LedgerException(400, "idempotency_key_required", "send an Idempotency-Key header with every write")

class IdempotencyKeyReused :
    LedgerException(422, "idempotency_key_reused", "this Idempotency-Key was used for a different request")

/** RFC 9457 problem details. */
fun problem(e: LedgerException): Map<String, Any?> = linkedMapOf(
    "type" to "https://github.com/kyan9400/ledgerline#${e.code}",
    "title" to e.code.replace('_', ' ').replaceFirstChar { it.uppercase() },
    "status" to e.status,
    "code" to e.code,
    "detail" to e.message,
) + e.details
