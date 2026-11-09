package dev.ledgerline.ledger

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.UUID

/**
 * Tests against a real PostgreSQL (LEDGER_TEST_DB_URL, e.g. jdbc:postgresql://host:5432/ledger_test?user=..&password=..).
 * The event log refuses TRUNCATE, so tests don't clean up: each uses accounts of its own (codes with a
 * random suffix), and the audit checks the whole database, every test's data included.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EnabledIfEnvironmentVariable(named = "LEDGER_TEST_DB_URL", matches = ".+")
abstract class DatabaseTest {
    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { System.getenv("LEDGER_TEST_DB_URL") }
            registry.add("spring.datasource.username") { System.getenv("LEDGER_TEST_DB_USER") ?: "ledger" }
            registry.add("spring.datasource.password") { System.getenv("LEDGER_TEST_DB_PASSWORD") ?: "ledger" }
            registry.add("spring.datasource.hikari.maximum-pool-size") { "32" }
        }
    }

    protected fun code(name: String) = "test:$name:${UUID.randomUUID().toString().take(8)}"
}
