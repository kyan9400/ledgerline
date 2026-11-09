package dev.ledgerline.ledger

import io.kotest.common.ExperimentalKotest
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.choice
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.map
import io.kotest.property.checkAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Random sequences of operations, run against the ledger and against a model of it a few lines long.
 * After every operation the outcome (done, or refused and why) and every balance must agree with the
 * model; after every sequence the audit must pass. Kotest shrinks a failing sequence to a short one.
 */
@OptIn(ExperimentalKotest::class) // PropTestConfig's fixed seed, so a run can be repeated
class PropertyTest : DatabaseTest() {
    @Autowired
    lateinit var ledger: Ledger

    @Autowired
    lateinit var idem: Idempotency

    @Autowired
    lateinit var audit: Audit

    @Autowired
    lateinit var json: tools.jackson.databind.json.JsonMapper

    sealed interface Op
    data class Transfer(val from: Int, val to: Int, val amount: Long) : Op
    data class Split(val from: Int, val amount: Long, val parts: Int) : Op // one debit, several credits
    data class Reverse(val nth: Int) : Op
    data class Retry(val nth: Int) : Op // repeat an earlier request with its key

    private val n = 5
    private val ops: Arb<Op> = Arb.choice(
        Arb.bind(Arb.int(0 until n), Arb.int(0 until n), Arb.long(1L..30_000L)) { a, b, x -> Transfer(a, b, x) },
        Arb.bind(Arb.int(0 until n), Arb.long(8L..30_000L), Arb.int(2..4)) { a, x, k -> Split(a, x, k) },
        Arb.int(0..30).map { Reverse(it) },
        Arb.int(0..30).map { Retry(it) },
    )

    @Test
    fun `the ledger behaves like its model`() {
        runBlocking { model() }
    }

    private suspend fun model() {
        checkAll(PropTestConfig(iterations = 150, seed = 20261109), Arb.list(ops, 1..40)) { seq ->
            val bank = ledger.openAccount(OpenAccount(code("bank"), "RUB", "system", allowNegative = true)).id
            val acc = (0 until n).map { ledger.openAccount(OpenAccount(code("p$it"), "RUB", "customer")).id }
            val model = LongArray(n) { 20_000 }
            acc.forEach { ledger.post(PostingRequest(listOf(EntryRequest(bank, -20_000), EntryRequest(it, 20_000)))) }

            data class Done(val key: String, val body: Any, val outcome: String, val txId: UUID?, val moves: Map<Int, Long>)
            val history = mutableListOf<Done>()
            val reversed = mutableSetOf<UUID>()

            fun apply(moves: Map<Int, Long>): String =
                if (moves.any { (i, d) -> model[i] + d < 0 }) "insufficient_funds" else {
                    moves.forEach { (i, d) -> model[i] += d }
                    "done"
                }

            for (op in seq) {
                when (op) {
                    is Transfer, is Split -> {
                        val (body, moves) = when (op) {
                            is Transfer -> {
                                val to = if (op.to == op.from) (op.to + 1) % n else op.to
                                TransferRequest(acc[op.from], acc[to], op.amount) to mapOf(op.from to -op.amount, to to op.amount)
                            }
                            else -> {
                                op as Split
                                val targets = (1..op.parts).map { (op.from + it) % n }.distinct()
                                val each = op.amount / targets.size
                                val moves = mapOf(op.from to -each * targets.size) + targets.associateWith { each }
                                PostingRequest(moves.map { (i, d) -> EntryRequest(acc[i], d) }) to moves
                            }
                        }
                        val key = UUID.randomUUID().toString()
                        val got = idem.run(key, "op", body) {
                            if (body is TransferRequest) ledger.transferIn(body) else ledger.postIn(body as PostingRequest)
                        }
                        val expected = apply(moves)
                        val outcome = if (got.status == 201) "done" else json.readTree(got.body)["code"].asString()
                        assertEquals(expected, outcome, "$op")
                        val txId = if (got.status == 201) UUID.fromString(json.readTree(got.body)["id"].asString()) else null
                        history += Done(key, body, outcome, txId, moves)
                    }
                    is Reverse -> {
                        val target = history.filter { it.txId != null }.getOrNull(op.nth) ?: continue
                        val expected = when {
                            target.txId in reversed -> "already_reversed"
                            else -> apply(target.moves.mapValues { -it.value })
                        }
                        val outcome = try {
                            ledger.reverse(target.txId!!)
                            "done"
                        } catch (e: LedgerException) {
                            e.code
                        }
                        assertEquals(expected, outcome, "$op of $target")
                        if (outcome == "done") reversed += target.txId!!
                    }
                    is Retry -> {
                        val earlier = history.getOrNull(op.nth) ?: continue
                        // Retried: the same answer, and the model doesn't move.
                        val got = idem.run(earlier.key, "op", earlier.body) { error("a retry must not run again") }
                        assertTrue(got.replayed)
                        assertEquals(earlier.outcome, if (got.status == 201) "done" else json.readTree(got.body)["code"].asString())
                    }
                }
                acc.forEachIndexed { i, id -> assertEquals(model[i], ledger.account(id).balance, "account $i after $op") }
            }
            assertTrue(audit.run().ok)
        }
    }
}
