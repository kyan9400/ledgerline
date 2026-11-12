package dev.ledgerline.payments

import dev.ledgerline.platform.Outbox
import dev.ledgerline.platform.Traces
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.ObservationRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.json.JsonMapper
import java.time.Clock
import java.time.Duration

@SpringBootApplication
class PaymentsApplication {
    @Bean
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun instrumentation(meters: MeterRegistry, observations: ObjectProvider<ObservationRegistry>) =
        Instrumentation(meters, observations.getIfAvailable { ObservationRegistry.NOOP })

    @Bean
    fun ledgerClient(
        @Value("\${payments.ledger.url}") url: String,
        @Value("\${payments.ledger.timeout}") timeout: Duration,
        json: JsonMapper,
        instrumentation: Instrumentation,
    ): LedgerClient = HttpService(url, timeout, json, instrumentation.http("ledger"))

    @Bean
    fun ledgerOps(client: LedgerClient) = LedgerOps(client)

    @Bean
    fun systemAccounts(ledger: LedgerOps, @Value("\${payments.system-account-shards}") shards: Int) = SystemAccounts(ledger, shards)

    /** The banks payouts can go to, by name. (One, for now: the simulated other bank.) */
    @Bean
    fun banks(
        @Value("\${payments.otherbank.url}") url: String,
        @Value("\${payments.otherbank.timeout}") timeout: Duration,
        json: JsonMapper,
        instrumentation: Instrumentation,
    ) = instrumentation.http("otherbank").let { http ->
        Banks(mapOf("otherbank" to BankClient(HttpService(url, timeout, json, http), HttpService(url, Duration.ofSeconds(30), json, http))))
    }

    /** The fraud scorer (payments.fraud.url; none: off). */
    @Bean
    fun fraudCheck(
        @Value("\${payments.fraud.url:}") url: String,
        @Value("\${payments.fraud.mode:off}") mode: String,
        @Value("\${payments.fraud.timeout:150ms}") timeout: Duration,
        json: JsonMapper,
        meters: MeterRegistry,
        instrumentation: Instrumentation,
    ) = FraudCheck(
        url.takeIf { it.isNotBlank() }?.let { HttpService(it, timeout, json, instrumentation.http("fraud")) },
        if (url.isBlank()) "off" else mode,
        meters,
    )

    @Bean
    fun idempotency(db: JdbcClient, json: JsonMapper, tx: TransactionTemplate) = Idempotency(db, json, tx)

    @Bean
    fun payments(
        db: JdbcClient,
        tx: TransactionTemplate,
        ledger: LedgerOps,
        accounts: SystemAccounts,
        clock: Clock,
        @Value("\${payments.authorisation-ttl}") ttl: Duration,
        fraud: FraudCheck,
        outbox: Outbox,
        json: JsonMapper,
        meters: MeterRegistry,
        webhooks: Webhooks,
        traces: Traces,
    ) = Payments(db, tx, ledger, accounts, clock, ttl, fraud, outbox, json, meters, webhooks, traces)

    @Bean
    fun payouts(
        db: JdbcClient,
        tx: TransactionTemplate,
        ledger: LedgerOps,
        accounts: SystemAccounts,
        banks: Banks,
        clock: Clock,
        outbox: Outbox,
        json: JsonMapper,
        meters: MeterRegistry,
        traces: Traces,
    ) = Payouts(db, tx, ledger, accounts, banks.byName, clock, outbox, json, meters, traces)

    @Bean
    fun merchants(db: JdbcClient, ledger: LedgerOps) = Merchants(db, ledger)

    @Bean
    fun webhooks(db: JdbcClient, merchants: Merchants, json: JsonMapper, clock: Clock, meters: MeterRegistry, traces: Traces) =
        Webhooks(db, merchants, json, clock, meters, traces)

    /** The merchants' gRPC API (payments.grpc.port; 0 picks a free port, for the tests). */
    @Bean
    fun merchantApi(payments: Payments, writes: Writes, merchants: Merchants, json: JsonMapper, @Value("\${outbox.kafka.bootstrap-servers:}") kafka: String) =
        MerchantApi(payments, writes, merchants, json, kafka)

    @Bean
    fun grpcServer(
        @Value("\${payments.grpc.port:9090}") port: Int,
        api: MerchantApi,
        merchants: Merchants,
        observations: ObjectProvider<ObservationRegistry>,
    ) = GrpcServer(port, api, merchants, observations.getIfAvailable { ObservationRegistry.NOOP })

    @Bean
    fun writes(payments: Payments, payouts: Payouts, idem: Idempotency, json: JsonMapper) = Writes(payments, payouts, idem, json)

    @Bean
    fun worker(
        payments: Payments,
        payouts: Payouts,
        webhooks: Webhooks,
        @Value("\${payments.worker.poll-ms}") pollMs: Long,
        @Value("\${payments.worker.parallelism}") parallelism: Int,
        meters: MeterRegistry,
        traces: Traces,
    ) = Worker(payments, payouts, webhooks, pollMs, parallelism, meters, traces)

    @Bean
    fun backlog(db: JdbcClient, meters: MeterRegistry) = Backlog(db, meters)

    @Bean
    fun reconcile(db: JdbcClient, tx: TransactionTemplate, ledger: LedgerOps, accounts: SystemAccounts, banks: Banks) =
        Reconcile(db, tx, ledger, accounts, banks.byName)
}

/** A holder, so Spring doesn't take a Map<String, BankClient> parameter to mean "all BankClient beans". */
class Banks(val byName: Map<String, BankClient>)

fun main(args: Array<String>) {
    runApplication<PaymentsApplication>(*args)
}
