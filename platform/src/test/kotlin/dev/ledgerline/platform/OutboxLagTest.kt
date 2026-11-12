package dev.ledgerline.platform

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.transaction.support.TransactionTemplate
import kotlin.test.assertTrue

/**
 * While Kafka can't be reached, `outbox.lag.seconds` must still grow: it is what the OutboxLagging alert
 * watches. (It didn't: the relay retried making its topics and never got as far as measuring, so the
 * alert evaluation's Kafka outage showed a lag of 0 throughout.)
 */
@EnabledIfEnvironmentVariable(named = "LEDGERLINE_TEST_DB", matches = ".+")
class OutboxLagTest {
    @Test
    fun `the lag grows while Kafka is unreachable`() {
        val ds = DriverManagerDataSource(
            System.getenv("LEDGERLINE_TEST_DB") + "/ledger_test?currentSchema=outbox_lag_test",
            System.getenv("LEDGERLINE_TEST_DB_USER") ?: "ledger",
            System.getenv("LEDGERLINE_TEST_DB_PASSWORD") ?: "ledger",
        )
        val db = JdbcClient.create(ds)
        db.sql("CREATE SCHEMA IF NOT EXISTS outbox_lag_test").update()
        db.sql("DROP TABLE IF EXISTS outbox_lag_test.outbox").update()
        db.sql(
            """CREATE TABLE outbox_lag_test.outbox (
                 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, topic text NOT NULL, key text NOT NULL,
                 event_id text NOT NULL, type text NOT NULL, payload text NOT NULL,
                 created_at timestamptz NOT NULL DEFAULT now(), trace_context text)""",
        ).update()
        db.sql("INSERT INTO outbox_lag_test.outbox (topic, key, event_id, type, payload, created_at) VALUES ('t', 'k', 'k:1', 'x', '{}', now() - interval '90 seconds')")
            .update()

        val meters = SimpleMeterRegistry()
        // Nothing listens on port 1: every attempt fails.
        val relay = OutboxRelay(db, TransactionTemplate(DataSourceTransactionManager(ds)), RelaySettings("127.0.0.1:1", listOf("t")), meters)
        relay.start()
        try {
            val deadline = System.nanoTime() + 60_000_000_000
            while ((meters.find("outbox.lag.seconds").gauge()?.value() ?: 0.0) < 90) {
                assertTrue(System.nanoTime() < deadline, "the lag should reach 90 s, the oldest event's age")
                Thread.sleep(200)
            }
        } finally {
            relay.stop()
            db.sql("DROP SCHEMA outbox_lag_test CASCADE").update()
        }
    }
}
