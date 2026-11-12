package dev.ledgerline.platform

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.core.Ordered
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.json.JsonMapper

/** What every Ledgerline service gets: the outbox and its relay, load shedding, and [Traces]. */
@AutoConfiguration(
    afterName = [
        "org.springframework.boot.jdbc.autoconfigure.JdbcClientAutoConfiguration",
        "org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration",
        "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
    ],
)
class PlatformAutoConfiguration {
    /** Over the service's tracer, when it has one (spring-boot-starter-opentelemetry); else no tracing. */
    @Bean
    fun traces(tracer: ObjectProvider<Tracer>, propagator: ObjectProvider<Propagator>) =
        Traces(tracer.getIfAvailable { Tracer.NOOP }, propagator.getIfAvailable { Propagator.NOOP })

    @Bean
    fun outbox(db: JdbcClient, json: JsonMapper, traces: Traces) = Outbox(db, json, traces)

    @Bean
    fun outboxRelay(
        db: JdbcClient,
        tx: TransactionTemplate,
        meters: MeterRegistry,
        traces: Traces,
        @Value("\${outbox.kafka.bootstrap-servers:}") bootstrap: String,
        @Value("\${outbox.topics:}") topics: List<String>,
        @Value("\${outbox.partitions:6}") partitions: Int,
    ) = OutboxRelay(db, tx, RelaySettings(bootstrap, topics.filter { it.isNotBlank() }, partitions), meters, traces)

    @Bean
    @ConditionalOnWebApplication
    fun databaseProbe(db: JdbcClient, meters: MeterRegistry) = DatabaseProbe(db, meters)

    @Bean
    @ConditionalOnWebApplication
    fun admission(
        @Value("\${admission.max-in-flight:32}") maxInFlight: Int,
        @Value("\${admission.wait-ms:200}") waitMs: Long,
        json: JsonMapper,
        meters: MeterRegistry,
    ) = FilterRegistrationBean(Admission(maxInFlight, waitMs, json, meters)).apply { order = Ordered.HIGHEST_PRECEDENCE }
}
