package dev.ledgerline.ledger

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Many writers at once, on the same accounts: what a lock or a check missed would show here. */
class ConcurrencyTest : DatabaseTest() {
    @Autowired
    lateinit var ledger: Ledger

    @Autowired
    lateinit var idem: Idempotency

    @Autowired
    lateinit var audit: Audit

    private fun parallel(threads: Int, tasks: Int, task: (Int) -> Unit) {
        val start = CountDownLatch(1)
        Executors.newFixedThreadPool(threads).use { pool ->
            val futures = (0 until tasks).map { i -> pool.submit { start.await(); task(i) } }
            start.countDown()
            futures.forEach { it.get() }
        }
    }

    private fun funded(name: String, amount: Long): Pair<UUID, UUID> {
        val bank = ledger.openAccount(OpenAccount(code("bank"), "RUB", "system", allowNegative = true)).id
        val acc = ledger.openAccount(OpenAccount(code(name), "RUB", "customer")).id
        ledger.post(PostingRequest(listOf(EntryRequest(bank, -amount), EntryRequest(acc, amount))))
        return bank to acc
    }

    @Test
    fun `400 transfers race for money for 100, and exactly 100 go through`() {
        val (_, alice) = funded("alice", 10_000)
        val bob = ledger.openAccount(OpenAccount(code("bob"), "RUB", "customer")).id
        val ok = AtomicInteger()
        val refused = AtomicInteger()
        parallel(threads = 32, tasks = 400) {
            try {
                ledger.transfer(TransferRequest(alice, bob, 100))
                ok.incrementAndGet()
            } catch (_: InsufficientFunds) {
                refused.incrementAndGet()
            }
        }
        assertEquals(100, ok.get())
        assertEquals(300, refused.get())
        assertEquals(0, ledger.account(alice).balance)
        assertEquals(10_000, ledger.account(bob).balance)
        assertEquals(102, ledger.account(alice).version) // opened, funded, 100 debits
        assertTrue(audit.run().ok)
    }

    @Test
    fun `transfers in every direction between ten accounts, and nothing is created or lost`() {
        val bank = ledger.openAccount(OpenAccount(code("bank"), "RUB", "system", allowNegative = true)).id
        val accounts = (1..10).map { ledger.openAccount(OpenAccount(code("acc$it"), "RUB", "customer")).id }
        accounts.forEach { ledger.post(PostingRequest(listOf(EntryRequest(bank, -50_000), EntryRequest(it, 50_000)))) }
        val outcomes = ConcurrentLinkedQueue<String>()
        parallel(threads = 32, tasks = 3000) { i ->
            val r = Random(i)
            val from = accounts[r.nextInt(10)]
            var to = accounts[r.nextInt(10)]
            if (to == from) to = accounts[(accounts.indexOf(from) + 1) % 10]
            try {
                ledger.transfer(TransferRequest(from, to, r.nextLong(1, 20_000)))
                outcomes.add("ok")
            } catch (_: InsufficientFunds) {
                outcomes.add("refused")
            }
        }
        val total = accounts.sumOf { ledger.account(it).balance }
        assertEquals(500_000, total)
        assertTrue(accounts.all { ledger.account(it).balance >= 0 })
        assertEquals(3000, outcomes.size)
        assertTrue(outcomes.count { it == "ok" } > 1000, "most transfers should go through")
        assertTrue(audit.run().ok)
    }

    @Test
    fun `30 requests with one idempotency key at once give one transaction and one answer`() {
        val (_, alice) = funded("alice", 1_000)
        val bob = ledger.openAccount(OpenAccount(code("bob"), "RUB", "customer")).id
        val key = UUID.randomUUID().toString()
        val body = TransferRequest(alice, bob, 250)
        val answers = ConcurrentLinkedQueue<Stored>()
        parallel(threads = 30, tasks = 30) { answers.add(idem.run(key, "transfer", body) { ledger.transferIn(body) }) }
        assertEquals(1, answers.count { !it.replayed })
        assertEquals(1, answers.map { it.body }.toSet().size)
        assertEquals(750, ledger.account(alice).balance)
        assertEquals(1, ledger.statement(bob, 10, null).entries.size) // the one credit
    }
}
