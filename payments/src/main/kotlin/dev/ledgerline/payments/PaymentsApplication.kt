package dev.ledgerline.payments

import dev.ledgerline.platform.Outbox
import io.micrometer.core.instrument.MeterRegistry
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
    fun ledgerClient(@Value("\${payments.ledger.url}") url: String, @Value("\${payments.ledger.timeout}") timeout: Duration, json: JsonMapper): LedgerClient =
        HttpService(url, timeout, json)

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
    ) = Banks(mapOf("otherbank" to BankClient(HttpService(url, timeout, json))))

    /** The fraud scorer (payments.fraud.url; none: off). */
    @Bean
    fun fraudCheck(
        @Value("\${payments.fraud.url:}") url: String,
        @Value("\${payments.fraud.mode:off}") mode: String,
        @Value("\${payments.fraud.timeout:150ms}") timeout: Duration,
        json: JsonMapper,
        meters: MeterRegistry,
    ) = FraudCheck(url.takeIf { it.isNotBlank() }?.let { HttpService(it, timeout, json) }, if (url.isBlank()) "off" else mode, meters)

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
    ) = Payments(db, tx, ledger, accounts, clock, ttl, fraud, outbox, json, meters)

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
    ) = Payouts(db, tx, ledger, accounts, banks.byName, clock, outbox, json, meters)

    @Bean
    fun worker(
        payments: Payments,
        payouts: Payouts,
        @Value("\${payments.worker.poll-ms}") pollMs: Long,
        @Value("\${payments.worker.parallelism}") parallelism: Int,
        meters: MeterRegistry,
    ) = Worker(payments, payouts, pollMs, parallelism, meters)

    @Bean
    fun reconcile(db: JdbcClient, tx: TransactionTemplate, ledger: LedgerOps, accounts: SystemAccounts, banks: Banks) =
        Reconcile(db, tx, ledger, accounts, banks.byName)
}

/** A holder, so Spring doesn't take a Map<String, BankClient> parameter to mean "all BankClient beans". */
class Banks(val byName: Map<String, BankClient>)

fun main(args: Array<String>) {
    runApplication<PaymentsApplication>(*args)
}
