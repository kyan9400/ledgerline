package dev.ledgerline.payments

import dev.ledgerline.platform.Outbox
import dev.ledgerline.platform.Traces
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.json.JsonMapper
import java.security.MessageDigest
import java.sql.Timestamp
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random

const val PAYMENTS_TOPIC = "payments.events"

/**
 * The payments service's own accounts in the ledger, per currency (and per bank, for settlement), opened
 * on first use:
 *
 * - holds: authorised card payments, until captured or voided;
 * - clearing: payouts on their way to another bank;
 * - settlement: what has been paid out through each bank, owed to it until the banks settle between
 *   themselves (outside this system).
 */
class SystemAccounts(private val ledger: LedgerOps, val shards: Int = 16) {
    private val ids = ConcurrentHashMap<String, UUID>()

    /**
     * Holds and clearing come in [shards] accounts each (`payments:holds:rub:0` ...): every posting locks
     * its accounts' rows, so one account per currency made every card payment wait for every other. A
     * payment or payout is given one at random and keeps it.
     */
    fun holds(currency: String, shard: Int) = get("payments:holds:${currency.lowercase()}:$shard", currency, allowNegative = false)

    fun clearing(currency: String, shard: Int) = get("payments:clearing:${currency.lowercase()}:$shard", currency, allowNegative = false)

    /** The single accounts from before there were shards: still used by the rows from then. */
    fun legacyHolds(currency: String) = get("payments:holds:${currency.lowercase()}", currency, allowNegative = false)

    fun legacyClearing(currency: String) = get("payments:clearing:${currency.lowercase()}", currency, allowNegative = false)

    fun settlement(bank: String, currency: String) = get("payments:settlement:$bank:${currency.lowercase()}", currency, allowNegative = false)

    private fun get(code: String, currency: String, allowNegative: Boolean): UUID = ids[code] ?: run {
        val id = when (val o = ledger.openAccount("payments:account:$code", code, currency, allowNegative)) {
            is Outcome.Done -> UUID.fromString(o.body["id"].asString())
            is Outcome.Refused ->
                if (o.code == "account_exists") ledger.accountByCode(code)!!.id else error("can't open $code: ${o.code} ${o.detail}")
            is Outcome.Unavailable -> throw LedgerUnavailable(o.reason)
        }
        ids.putIfAbsent(code, id) ?: id
    }
}

/** An answer to a write: the status and the exact body, first time or replayed. */
data class Answer(val status: Int, val body: String, val replayed: Boolean)

/**
 * Runs a write at most once per idempotency key, as in the ledger: the key is claimed with an INSERT in
 * the write's database transaction, a retry gets the stored answer (a refusal included), and the same key
 * with a different request is refused. A 5xx is not an answer: it rolls back, and the key is free again.
 *
 * The write here only records the intent (the payment, in a state ending in -ing). The step itself calls
 * the ledger after that commits, and [update] then stores the answer the client got. If the service dies
 * in between, a retry gets the intent (202) and the worker finishes the step.
 */
class Idempotency(private val db: JdbcClient, private val json: JsonMapper, private val tx: TransactionTemplate) {

    fun run(key: String?, operation: String, request: Any, write: () -> Pair<Int, Any>): Answer {
        if (key.isNullOrBlank()) throw PaymentsException(400, "idempotency_key_required", "send an Idempotency-Key header with every write")
        if (key.length > 200) throw Invalid("Idempotency-Key is at most 200 characters")
        val hash = sha256("$operation\n${json.writeValueAsString(request)}")
        return tx.execute {
            val claimed = db.sql("INSERT INTO idempotency_keys (key, request_hash, status, response) VALUES (:k, :h, 0, '') ON CONFLICT (key) DO NOTHING")
                .param("k", key).param("h", hash).update() == 1
            if (!claimed) {
                val (storedHash, status, body) = db.sql("SELECT request_hash, status, response FROM idempotency_keys WHERE key = :k")
                    .param("k", key).query { rs, _ -> Triple(rs.getString(1), rs.getInt(2), rs.getString(3)) }.single()
                if (storedHash != hash) throw PaymentsException(422, "idempotency_key_reused", "this Idempotency-Key was used for a different request")
                return@execute Answer(status, body, replayed = true)
            }
            val (status, body) = try {
                write().let { (s, b) -> s to json.writeValueAsString(b) }
            } catch (e: PaymentsException) {
                if (e.status >= 500) throw e
                e.status to json.writeValueAsString(problem(e))
            }
            store(key, status, body)
            Answer(status, body, replayed = false)
        }
    }

    fun update(key: String, status: Int, body: String) = store(key, status, body)

    private fun store(key: String, status: Int, body: String) {
        db.sql("UPDATE idempotency_keys SET status = :s, response = :r WHERE key = :k").param("s", status).param("r", body).param("k", key).update()
    }

    private fun sha256(s: String) = MessageDigest.getInstance("SHA-256").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }
}

/** A row due for its next step, and the trace its last change was made in. */
data class Due<K : Any>(val id: K, val trace: String?)

/** What one attempt at a step came to. */
sealed interface Step {
    /** Move to the next state, setting these columns too. */
    data class To(val state: String, val set: Map<String, Any?> = emptyMap()) : Step

    /** It didn't get an answer: try again after a backoff. */
    data class Retry(val error: String) : Step

    /** Nothing to do yet (the bank hasn't settled): look again later. */
    data class Later(val delay: Duration) : Step

    /** A refusal that shouldn't happen (the ledger refused to release a hold): needs a person. */
    data class Stuck(val error: String) : Step
}

/**
 * Moves the rows of one table (payments, refunds, payouts) from state to state. A change is conditional
 * on the version it was decided from, so when two drivers race (the request and the worker, after a
 * lease ran out) only one change lands; both made the same ledger call with the same key, and got the
 * same answer. Each change appends an event to the service's event log, and puts it in the outbox, in
 * the same transaction.
 */
class Machine(
    private val db: JdbcClient,
    private val outbox: Outbox,
    private val json: JsonMapper,
    private val clock: Clock,
    private val meters: MeterRegistry,
    private val table: String,
    private val entity: String,
    private val steps: Set<String>,
    private val snapshot: (UUID) -> Any,
    private val webhooks: Webhooks? = null,
    private val traces: Traces = Traces.NONE,
) {
    /** The current trace, for a new row's trace_context. */
    fun trace(): String? = traces.current()

    /** In the caller's transaction. A step state is picked up by the worker at once, or after the lease. */
    fun change(id: UUID, fromVersion: Int, to: String, set: Map<String, Any?> = emptyMap(), lease: Boolean = false): Boolean {
        val now = clock.instant()
        val next = if (to in steps) now.plus(if (lease) LEASE else Duration.ZERO) else null
        val extra = set.keys.joinToString("") { ", $it = :$it" }
        val sql = db.sql(
            """UPDATE $table SET state = :to, version = version + 1, attempts = 0, last_error = NULL,
                 next_attempt_at = :next, updated_at = :now, trace_context = coalesce(:tc::text, trace_context)$extra
               WHERE id = :id AND version = :v""",
        ).param("to", to).param("tc", traces.current()).param("next", next?.let(Timestamp::from)).param("now", Timestamp.from(now))
            .param("id", id).param("v", fromVersion)
        set.forEach { (k, v) -> sql.param(k, if (v is java.time.Instant) Timestamp.from(v) else v) }
        if (sql.update() == 0) return false
        record(id, fromVersion + 1, to, now)
        meters.counter("payments.transitions", "entity", entity, "to", to).increment()
        return true
    }

    /** The first event of a new row (version 1), in the caller's transaction. */
    fun created(id: UUID, state: String) {
        record(id, 1, state, clock.instant())
        meters.counter("payments.transitions", "entity", entity, "to", state).increment()
    }

    private fun record(id: UUID, version: Int, state: String, at: java.time.Instant) {
        val stream = "$entity-$id"
        val snap = snapshot(id)
        val data = json.writeValueAsString(snap)
        db.sql("INSERT INTO events (stream, version, type, data, recorded_at) VALUES (:s, :v, :t, :d::jsonb, :at)")
            .param("s", stream).param("v", version).param("t", "$entity.$state").param("d", data).param("at", Timestamp.from(at)).update()
        outbox.addJson(PAYMENTS_TOPIC, stream, version, "$entity.$state", data, at)
        // The merchant's webhook, queued in this same transaction.
        val merchant = when (snap) {
            is Payment -> snap.merchant
            is Refund -> snap.merchant
            else -> null
        }
        if (merchant != null) webhooks?.queue(merchant, "$stream:$version", "$entity.$state", version, data, at)
    }

    fun retry(id: UUID, version: Int, attempts: Int, error: String) {
        val delay = backoff(attempts)
        db.sql("UPDATE $table SET attempts = attempts + 1, last_error = :e, next_attempt_at = :next WHERE id = :id AND version = :v")
            .param("e", error.take(500)).param("next", Timestamp.from(clock.instant().plus(delay))).param("id", id).param("v", version).update()
        meters.counter("payments.retries", "entity", entity).increment()
    }

    fun later(id: UUID, version: Int, delay: Duration) {
        db.sql("UPDATE $table SET attempts = attempts + 1, last_error = NULL, next_attempt_at = :next WHERE id = :id AND version = :v")
            .param("next", Timestamp.from(clock.instant().plus(delay))).param("id", id).param("v", version).update()
    }

    fun stuck(id: UUID, version: Int, error: String) {
        db.sql("UPDATE $table SET attempts = attempts + 1, last_error = :e, next_attempt_at = NULL WHERE id = :id AND version = :v")
            .param("e", error.take(500)).param("id", id).param("v", version).update()
        meters.counter("payments.stuck", "entity", entity).increment()
    }

    /** Due rows, each leased to this worker so another doesn't take it meanwhile; with their traces. */
    fun claimDue(limit: Int): List<Due<UUID>> = db.sql(
        """UPDATE $table SET next_attempt_at = :lease WHERE id IN (
             SELECT id FROM $table WHERE next_attempt_at <= :now ORDER BY next_attempt_at LIMIT :n FOR UPDATE SKIP LOCKED)
           RETURNING id, trace_context""",
    ).param("lease", Timestamp.from(clock.instant().plus(LEASE))).param("now", Timestamp.from(clock.instant())).param("n", limit)
        .query { rs, _ -> Due(rs.getObject(1, UUID::class.java), rs.getString(2)) }.list()

    companion object {
        /** Longer than any one step can take (each call has a timeout of a few seconds). */
        val LEASE: Duration = Duration.ofSeconds(10)

        /** 200 ms, doubling, at most 30 s, with jitter so retries after an outage don't arrive together. */
        fun backoff(attempts: Int): Duration {
            val base = (200L shl attempts.coerceIn(0, 8)).coerceAtMost(30_000)
            return Duration.ofMillis((base * (0.5 + Random.nextDouble())).toLong())
        }
    }
}
