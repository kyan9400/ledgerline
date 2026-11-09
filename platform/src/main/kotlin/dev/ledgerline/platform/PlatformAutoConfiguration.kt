package dev.ledgerline.platform

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.core.Ordered
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.json.JsonMapper

/** What every Ledgerline service gets: the outbox and its relay, and load shedding. */
@AutoConfiguration(
    afterName = [
        "org.springframework.boot.jdbc.autoconfigure.JdbcClientAutoConfiguration",
        "org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration",
        "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
    ],
)
class PlatformAutoConfiguration {
    @Bean
    fun outbox(db: JdbcClient, json: JsonMapper) = Outbox(db, json)

    @Bean
    fun outboxRelay(
        db: JdbcClient,
        tx: TransactionTemplate,
        meters: MeterRegistry,
        @Value("\${outbox.kafka.bootstrap-servers:}") bootstrap: String,
        @Value("\${outbox.topics:}") topics: List<String>,
        @Value("\${outbox.partitions:6}") partitions: Int,
    ) = OutboxRelay(db, tx, RelaySettings(bootstrap, topics.filter { it.isNotBlank() }, partitions), meters)

    @Bean
    @ConditionalOnWebApplication
    fun admission(
        @Value("\${admission.max-in-flight:32}") maxInFlight: Int,
        @Value("\${admission.wait-ms:200}") waitMs: Long,
        json: JsonMapper,
        meters: MeterRegistry,
    ) = FilterRegistrationBean(Admission(maxInFlight, waitMs, json, meters)).apply { order = Ordered.HIGHEST_PRECEDENCE }
}
