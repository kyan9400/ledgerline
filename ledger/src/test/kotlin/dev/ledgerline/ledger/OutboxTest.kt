package dev.ledgerline.ledger

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.simple.JdbcClient
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The outbox, end to end: the ledger's database, the relay, and a real Kafka (LEDGERLINE_TEST_KAFKA). */
@EnabledIfEnvironmentVariable(named = "LEDGERLINE_TEST_KAFKA", matches = ".+")
class OutboxTest : DatabaseTest() {
    @Autowired
    lateinit var ledger: Ledger

    @Autowired
    lateinit var db: JdbcClient

    @Autowired
    lateinit var json: JsonMapper

    @Test
    fun `every event reaches Kafka, in order per stream, and the balances rebuild from it`() {
        val bank = ledger.openAccount(OpenAccount(code("bank"), "RUB", "system", allowNegative = true))
        val accounts = (1..20).map { ledger.openAccount(OpenAccount(code("c$it"), "RUB", "customer")) }
        accounts.forEach { ledger.post(PostingRequest(listOf(EntryRequest(bank.id, -100_000), EntryRequest(it.id, 100_000)))) }
        val pool = Executors.newFixedThreadPool(16)
        val random = Random(20261109)
        repeat(2_000) {
            val from = accounts[random.nextInt(accounts.size)].id
            val to = accounts.filter { it.id != from }[random.nextInt(accounts.size - 1)].id
            val amount = 1L + random.nextLong(20_000)
            pool.execute {
                try {
                    ledger.transfer(TransferRequest(from, to, amount))
                } catch (_: InsufficientFunds) {
                }
            }
        }
        pool.shutdown()
        assertTrue(pool.awaitTermination(2, TimeUnit.MINUTES))

        val streams = (accounts + bank).associateBy { "account-${it.id}" }
        val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos()
        while (db.sql("SELECT count(*) FROM outbox WHERE key IN (:s)").param("s", streams.keys).query(Long::class.java).single() > 0) {
            assertTrue(System.nanoTime() < deadline, "the outbox drains")
            Thread.sleep(100)
        }

        // Rebuild every account from the topic, as a consumer would: per stream, the next version is
        // applied, one already seen is skipped (delivery is at least once), and a gap is a failure.
        val version = mutableMapOf<String, Int>()
        val balance = mutableMapOf<String, Long>()
        var duplicates = 0
        for ((key, payload) in readAll(LEDGER_TOPIC)) {
            if (key !in streams) continue
            val event = json.readTree(payload)
            val v = event["version"].asInt()
            val last = version[key] ?: 0
            if (v <= last) {
                duplicates++
                continue
            }
            assertEquals(last + 1, v, "$key: versions arrive in order, without gaps")
            assertEquals("$key:$v", event["id"].asString())
            version[key] = v
            if (event["type"].asString() == "Posted") balance.merge(key, event["data"]["amount"].asLong(), Long::plus)
        }
        for ((stream, account) in streams) {
            val now = ledger.account(account.id)
            assertEquals(now.version, version[stream], "$stream: every event arrived")
            assertEquals(now.balance, balance[stream] ?: 0L, "$stream: the balance rebuilt from Kafka is the ledger's")
        }
        println("rebuilt ${streams.size} accounts from Kafka; duplicates skipped: $duplicates")
    }

    private fun readAll(topic: String): List<Pair<String, String>> {
        val props = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to System.getenv("LEDGERLINE_TEST_KAFKA"),
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        )
        KafkaConsumer(props, StringDeserializer(), StringDeserializer()).use { consumer ->
            val partitions = consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
            consumer.assign(partitions)
            consumer.seekToBeginning(partitions)
            val end = consumer.endOffsets(partitions)
            val out = mutableListOf<Pair<String, String>>()
            while (partitions.any { consumer.position(it) < end.getValue(it) }) {
                consumer.poll(Duration.ofMillis(500)).forEach { out += it.key() to it.value() }
            }
            return out
        }
    }
}
