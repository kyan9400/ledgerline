package dev.ledgerline.ledger

import dev.ledgerline.platform.Outbox
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.json.JsonMapper
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Clock
import java.time.Instant
import java.util.Currency
import java.util.UUID

const val MAX_ENTRIES = 100
const val MAX_DESCRIPTION = 500
const val MAX_METADATA = 20

/** The Kafka topic every ledger event goes to, through the outbox (keyed by stream). */
const val LEDGER_TOPIC = "ledger.events"

/**
 * The ledger: double-entry postings over an append-only event log.
 *
 * A posting locks the balances of its accounts in id order (so two postings over the same accounts can't
 * deadlock), checks that each currency sums to zero and that no account that may not go negative would,
 * then appends one event to the transaction's stream and one to each account's stream, and updates the
 * projections, all in one database transaction. Each event also goes into the outbox, in that same
 * transaction, on its way to Kafka. The methods ending in `In` run inside the caller's transaction (the
 * idempotency layer opens it); the others open their own.
 */
@Service
class Ledger(
    private val db: JdbcClient,
    private val json: JsonMapper,
    private val tx: TransactionTemplate,
    private val clock: Clock,
    private val meters: MeterRegistry,
    private val outbox: Outbox,
) {
    // --- accounts ------------------------------------------------------------------------------------

    fun openAccountIn(req: OpenAccount): Account {
        if (!Regex("^[a-z0-9][a-z0-9:_.-]{0,99}$").matches(req.code)) {
            throw Invalid("code must be 1-100 of a-z, 0-9 and : _ . - (starting with a letter or digit)")
        }
        val currency = currency(req.currency)
        if (req.kind !in setOf("customer", "merchant", "system")) throw Invalid("kind must be customer, merchant or system")
        if (req.allowNegative && req.kind != "system") throw Invalid("only system accounts may go below zero")
        val id = UUID.randomUUID()
        val now = clock.instant()
        val inserted = db.sql(
            """INSERT INTO accounts (id, code, currency, kind, allow_negative, opened_at)
               VALUES (:id, :code, :currency, :kind, :neg, :at) ON CONFLICT (code) DO NOTHING""",
        ).param("id", id).param("code", req.code).param("currency", currency).param("kind", req.kind)
            .param("neg", req.allowNegative).param("at", Timestamp.from(now)).update()
        if (inserted == 0) throw Conflict("account_exists", "an account with code ${req.code} exists")
        append(
            "account-$id", 1, "AccountOpened",
            mapOf("code" to req.code, "currency" to currency, "kind" to req.kind, "allowNegative" to req.allowNegative),
            now,
        )
        db.sql("INSERT INTO balances (account_id, balance, version, allow_negative, updated_at) VALUES (:id, 0, 1, :neg, :at)")
            .param("id", id).param("neg", req.allowNegative).param("at", Timestamp.from(now)).update()
        return Account(id, req.code, currency, req.kind, req.allowNegative, 0, 1, now)
    }

    fun account(id: UUID): Account = findAccount("a.id = :v", id) ?: throw NotFound("account $id")

    fun accountByCode(code: String): Account = findAccount("a.code = :v", code) ?: throw NotFound("account $code")

    private fun findAccount(where: String, value: Any): Account? = db.sql(
        """SELECT a.id, a.code, a.currency, a.kind, a.allow_negative, a.opened_at, b.balance, b.version
           FROM accounts a JOIN balances b ON b.account_id = a.id WHERE $where""",
    ).param("v", value).query { rs, _ ->
        Account(
            rs.getObject("id", UUID::class.java), rs.getString("code"), rs.getString("currency"), rs.getString("kind"),
            rs.getBoolean("allow_negative"), rs.getLong("balance"), rs.getInt("version"),
            rs.getTimestamp("opened_at").toInstant(),
        )
    }.optional().orElse(null)

    fun statement(id: UUID, limit: Int, before: Int?): Statement {
        account(id)
        val n = limit.coerceIn(1, 500)
        val rows = db.sql(
            """SELECT transaction_id, amount, balance_after, account_version, recorded_at FROM entries
               WHERE account_id = :id AND (:before::integer IS NULL OR account_version < :before)
               ORDER BY account_version DESC LIMIT :n""",
        ).param("id", id).param("before", before).param("n", n + 1).query { rs, _ ->
            StatementEntry(
                rs.getObject("transaction_id", UUID::class.java), rs.getLong("amount"), rs.getLong("balance_after"),
                rs.getInt("account_version"), rs.getTimestamp("recorded_at").toInstant(),
            )
        }.list()
        val page = rows.take(n)
        return Statement(id, page, if (rows.size > n) page.last().version else null)
    }

    // --- postings ------------------------------------------------------------------------------------

    fun transferIn(req: TransferRequest): Transaction {
        if (req.amount <= 0) throw Invalid("amount must be positive")
        if (req.from == req.to) throw Invalid("from and to must differ")
        return postIn(
            PostingRequest(
                listOf(EntryRequest(req.from, -req.amount), EntryRequest(req.to, req.amount)),
                req.description,
                req.metadata,
            ),
            kind = "transfer",
        )
    }

    fun postIn(req: PostingRequest, kind: String = "posting", reverses: UUID? = null): Transaction {
        validate(req)
        val ids = req.entries.map { it.account }.sorted()
        // Locked in id order: two postings over the same accounts wait for each other instead of deadlocking.
        val locked = db.sql(
            """SELECT b.account_id, a.currency, b.allow_negative, b.balance, b.version
               FROM balances b JOIN accounts a ON a.id = b.account_id
               WHERE b.account_id IN (:ids) ORDER BY b.account_id FOR UPDATE OF b""",
        ).param("ids", ids).query { rs, _ -> Locked(rs) }.list().associateBy { it.id }
        ids.firstOrNull { it !in locked }?.let { throw NotFound("account $it") }

        val sums = mutableMapOf<String, Long>()
        for (e in req.entries) {
            val c = locked.getValue(e.account).currency
            sums[c] = Math.addExact(sums[c] ?: 0L, e.amount)
        }
        sums.filterValues { it != 0L }.takeIf { it.isNotEmpty() }?.let {
            throw Invalid("entries don't balance: per currency they must sum to zero", mapOf("sums" to sums))
        }
        val after = req.entries.associate { e ->
            val a = locked.getValue(e.account)
            val next = Math.addExact(a.balance, e.amount)
            if (next < 0 && !a.allowNegative) {
                meters.counter("ledger.refusals", "reason", "insufficient_funds").increment()
                throw InsufficientFunds(e.account, a.balance, e.amount)
            }
            e.account to next
        }

        val id = UUID.randomUUID()
        val now = clock.instant()
        val entries = req.entries.sortedBy { it.account }.map {
            TransactionEntry(it.account, locked.getValue(it.account).currency, it.amount, after.getValue(it.account))
        }
        append(
            "transaction-$id", 1, "TransactionRecorded",
            mapOf(
                "kind" to kind, "reverses" to reverses, "description" to req.description, "metadata" to req.metadata,
                "entries" to entries.map { mapOf("account" to it.account, "currency" to it.currency, "amount" to it.amount) },
            ),
            now,
        )
        db.sql(
            """INSERT INTO transactions (id, kind, reverses, description, metadata, recorded_at)
               VALUES (:id, :kind, :rev, :d, :m::jsonb, :at)""",
        ).param("id", id).param("kind", kind).param("rev", reverses).param("d", req.description)
            .param("m", json.writeValueAsString(req.metadata)).param("at", Timestamp.from(now)).update()
        for (e in entries) {
            val version = locked.getValue(e.account).version + 1
            append("account-${e.account}", version, "Posted", mapOf("transaction" to id, "amount" to e.amount, "balanceAfter" to e.balanceAfter), now)
            db.sql("UPDATE balances SET balance = :b, version = :v, updated_at = :at WHERE account_id = :id")
                .param("b", e.balanceAfter).param("v", version).param("at", Timestamp.from(now)).param("id", e.account).update()
            db.sql(
                """INSERT INTO entries (transaction_id, account_id, amount, balance_after, account_version, recorded_at)
                   VALUES (:t, :a, :amt, :after, :v, :at)""",
            ).param("t", id).param("a", e.account).param("amt", e.amount).param("after", e.balanceAfter)
                .param("v", version).param("at", Timestamp.from(now)).update()
        }
        meters.counter("ledger.transactions", "kind", kind).increment()
        return Transaction(id, kind, reverses, req.description, req.metadata, now, entries)
    }

    /** A new transaction with every entry negated. A transaction is reversed at most once. */
    fun reverseIn(id: UUID, description: String): Transaction {
        val original = db.sql("SELECT kind FROM transactions WHERE id = :id FOR UPDATE")
            .param("id", id).query(String::class.java).optional().orElseThrow { NotFound("transaction $id") }
        if (original == "reversal") throw Conflict("not_reversible", "a reversal can't itself be reversed")
        val by = db.sql("SELECT id FROM transactions WHERE reverses = :id").param("id", id)
            .query(UUID::class.java).optional()
        if (by.isPresent) throw Conflict("already_reversed", "transaction $id was reversed by ${by.get()}")
        val entries = transaction(id).entries.map { EntryRequest(it.account, -it.amount) }
        return postIn(PostingRequest(entries, description.ifBlank { "reversal of $id" }), kind = "reversal", reverses = id)
    }

    fun transaction(id: UUID): Transaction {
        val head = db.sql("SELECT id, kind, reverses, description, metadata, recorded_at FROM transactions WHERE id = :id")
            .param("id", id).query { rs, _ ->
                Transaction(
                    rs.getObject("id", UUID::class.java), rs.getString("kind"), rs.getObject("reverses", UUID::class.java),
                    rs.getString("description"), metadata(rs.getString("metadata")), rs.getTimestamp("recorded_at").toInstant(),
                    emptyList(),
                )
            }.optional().orElseThrow { NotFound("transaction $id") }
        val entries = db.sql(
            """SELECT e.account_id, a.currency, e.amount, e.balance_after FROM entries e
               JOIN accounts a ON a.id = e.account_id WHERE e.transaction_id = :id ORDER BY e.account_id""",
        ).param("id", id).query { rs, _ ->
            TransactionEntry(rs.getObject("account_id", UUID::class.java), rs.getString("currency"), rs.getLong("amount"), rs.getLong("balance_after"))
        }.list()
        return head.copy(entries = entries)
    }

    // --- in its own transaction (tests, tools) ----------------------------------------------------------

    fun openAccount(req: OpenAccount): Account = tx.execute { openAccountIn(req) }

    fun post(req: PostingRequest): Transaction = tx.execute { postIn(req) }

    fun transfer(req: TransferRequest): Transaction = tx.execute { transferIn(req) }

    fun reverse(id: UUID, description: String = ""): Transaction = tx.execute { reverseIn(id, description) }

    // --- helpers --------------------------------------------------------------------------------------

    private class Locked(rs: ResultSet) {
        val id: UUID = rs.getObject("account_id", UUID::class.java)
        val currency: String = rs.getString("currency")
        val allowNegative = rs.getBoolean("allow_negative")
        val balance = rs.getLong("balance")
        val version = rs.getInt("version")
    }

    private fun validate(req: PostingRequest) {
        if (req.entries.size !in 2..MAX_ENTRIES) throw Invalid("a posting needs 2 to $MAX_ENTRIES entries")
        if (req.entries.any { it.amount == 0L }) throw Invalid("an entry's amount can't be zero")
        if (req.entries.map { it.account }.toSet().size != req.entries.size) throw Invalid("each account at most once per posting")
        if (req.description.length > MAX_DESCRIPTION) throw Invalid("description is at most $MAX_DESCRIPTION characters")
        if (req.metadata.size > MAX_METADATA || req.metadata.any { (k, v) -> k.length > 40 || v.length > 500 }) {
            throw Invalid("metadata: at most $MAX_METADATA keys, keys up to 40 characters, values up to 500")
        }
    }

    private fun currency(code: String): String = try {
        Currency.getInstance(code).currencyCode
    } catch (_: IllegalArgumentException) {
        throw Invalid("unknown currency $code (ISO 4217, e.g. RUB)")
    }

    @Suppress("UNCHECKED_CAST")
    private fun metadata(s: String): Map<String, String> = json.readValue(s, Map::class.java) as Map<String, String>

    private fun append(stream: String, version: Int, type: String, data: Map<String, Any?>, at: Instant) {
        val dataJson = json.writeValueAsString(data)
        db.sql("INSERT INTO events (stream, version, type, data, recorded_at) VALUES (:s, :v, :t, :d::jsonb, :at)")
            .param("s", stream).param("v", version).param("t", type).param("d", dataJson)
            .param("at", Timestamp.from(at)).update()
        outbox.addJson(LEDGER_TOPIC, stream, version, type, dataJson, at)
    }
}
