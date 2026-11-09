package dev.ledgerline.ledger

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.UUID

/**
 * Tests against a real PostgreSQL: LEDGERLINE_TEST_DB is a JDBC URL without the database
 * (jdbc:postgresql://host:5432), and the database ledger_test must exist. With LEDGERLINE_TEST_KAFKA
 * (host:port) the outbox relay sends to Kafka too.
 *
 * The event log refuses TRUNCATE, so tests don't clean up: each uses accounts of its own (codes with a
 * random suffix), and the audit checks the whole database, every test's data included.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EnabledIfEnvironmentVariable(named = "LEDGERLINE_TEST_DB", matches = ".+")
abstract class DatabaseTest {
    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { System.getenv("LEDGERLINE_TEST_DB") + "/ledger_test" }
            registry.add("spring.datasource.username") { System.getenv("LEDGERLINE_TEST_DB_USER") ?: "ledger" }
            registry.add("spring.datasource.password") { System.getenv("LEDGERLINE_TEST_DB_PASSWORD") ?: "ledger" }
            registry.add("spring.datasource.hikari.maximum-pool-size") { "32" }
            registry.add("outbox.kafka.bootstrap-servers") { System.getenv("LEDGERLINE_TEST_KAFKA") ?: "" }
        }
    }

    protected fun code(name: String) = "test:$name:${UUID.randomUUID().toString().take(8)}"
}
