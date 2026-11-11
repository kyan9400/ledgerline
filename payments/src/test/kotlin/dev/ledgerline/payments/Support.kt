package dev.ledgerline.payments

import dev.ledgerline.ledger.LedgerApplication
import dev.ledgerline.otherbank.Faults
import dev.ledgerline.otherbank.OtherBank
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The world the payments tests run in, started once per JVM: a real ledger (the ledger module's
 * application, on its own port and database, ledger_test) and the simulated other bank.
 */
object World {
    private val db = System.getenv("LEDGERLINE_TEST_DB") ?: ""
    private val user = System.getenv("LEDGERLINE_TEST_DB_USER") ?: "ledger"
    private val password = System.getenv("LEDGERLINE_TEST_DB_PASSWORD") ?: "ledger"

    val bank: OtherBank by lazy { OtherBank(0, Faults(settleMinMs = 50, settleMaxMs = 300)).start() }

    val ledgerUrl: String by lazy {
        val ctx = SpringApplicationBuilder(LedgerApplication::class.java).run(
            // No config file: the payments tests' classpath has the payments service's application.yaml.
            "--spring.config.name=no-config-file",
            "--server.port=0",
            "--spring.datasource.url=$db/ledger_test",
            "--spring.datasource.username=$user",
            "--spring.datasource.password=$password",
            "--spring.datasource.hikari.maximum-pool-size=32",
            "--spring.threads.virtual.enabled=true",
            "--admission.max-in-flight=64",
            "--outbox.kafka.bootstrap-servers=",
        )
        "http://127.0.0.1:${ctx.environment.getProperty("local.server.port")}"
    }

    val bankUrl: String get() = "http://127.0.0.1:${bank.port}"

    fun dbUrl() = "$db/payments_test"

    fun user() = user

    fun password() = password
}

// Each test class's context (and its worker) is closed after it: a worker left running would finish the
// steps the next class's faults are meant to make hard.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EnabledIfEnvironmentVariable(named = "LEDGERLINE_TEST_DB", matches = ".+")
abstract class PaymentsTest {
    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { World.dbUrl() }
            registry.add("spring.datasource.username") { World.user() }
            registry.add("spring.datasource.password") { World.password() }
            registry.add("spring.datasource.hikari.maximum-pool-size") { "32" }
            registry.add("payments.ledger.url") { World.ledgerUrl }
            registry.add("payments.otherbank.url") { World.bankUrl }
            registry.add("payments.authorisation-ttl") { "5s" }
            registry.add("admission.max-in-flight") { "64" }
            registry.add("payments.grpc.port") { "0" }
            registry.add("outbox.kafka.bootstrap-servers") { System.getenv("LEDGERLINE_TEST_KAFKA") ?: "" }
        }
    }

    @Value("\${local.server.port}")
    var port: Int = 0

    @Autowired
    lateinit var json: JsonMapper

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

    data class Reply(val status: Int, val body: JsonNode, val raw: String, val replayed: Boolean)

    fun call(method: String, path: String, body: Any? = null, key: String? = UUID.randomUUID().toString(), base: String? = null): Reply {
        val req = HttpRequest.newBuilder(URI("${base ?: "http://127.0.0.1:$port"}$path"))
            .timeout(Duration.ofSeconds(30))
            .header("Content-Type", "application/json")
            .apply { if (key != null) header("Idempotency-Key", key) }
            .method(method, if (body == null) HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)))
            .build()
        val res = http.send(req, HttpResponse.BodyHandlers.ofString())
        return Reply(res.statusCode(), json.readTree(res.body().ifEmpty { "{}" }), res.body(), res.headers().firstValue("Idempotent-Replayed").orElse("") == "true")
    }

    fun get(path: String) = call("GET", path, key = null)

    fun ledger(method: String, path: String, body: Any? = null) = call(method, path, body, base = World.ledgerUrl)

    private val issuance by lazy { open("issuance", "system", negative = true) }

    fun code(name: String) = "test:$name:${UUID.randomUUID().toString().take(8)}"

    fun open(name: String, kind: String = "customer", negative: Boolean = false, funded: Long = 0): UUID {
        val r = ledger("POST", "/v1/accounts", mapOf("code" to code(name), "currency" to "RUB", "kind" to kind, "allowNegative" to negative))
        check(r.status == 201) { "open $name: ${r.raw}" }
        val id = UUID.fromString(r.body["id"].asString())
        if (funded > 0) {
            val t = ledger("POST", "/v1/transfers", mapOf("from" to issuance, "to" to id, "amount" to funded))
            check(t.status == 201) { "fund $name: ${t.raw}" }
        }
        return id
    }

    fun balance(id: UUID): Long = call("GET", "/v1/accounts/$id", key = null, base = World.ledgerUrl).body["balance"].asLong()

    fun systemAccount(code: String): UUID =
        UUID.fromString(call("GET", "/v1/accounts?code=$code", key = null, base = World.ledgerUrl).body["id"].asString())

    fun eventually(timeout: Duration = Duration.ofSeconds(30), what: String, check: () -> Boolean) {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (!check()) {
            if (System.nanoTime() > deadline) fail("timed out waiting: $what")
            Thread.sleep(50)
        }
    }

    /** Waits until nothing is in progress, then returns the reconciliation; it must hold. */
    fun reconciled(timeout: Duration = Duration.ofSeconds(90)): JsonNode {
        val deadline = System.nanoTime() + timeout.toNanos()
        var last = get("/internal/reconcile")
        // A 503 (the ledger didn't answer the reconciliation's own lookups) has no report: ask again.
        while (last.body["quiet"]?.asBoolean() != true) {
            if (System.nanoTime() > deadline) fail("still in progress after $timeout: ${last.raw}")
            Thread.sleep(100)
            last = get("/internal/reconcile")
        }
        assertTrue(last.body["ok"].asBoolean(), "reconciliation: ${last.raw}")
        return last.body
    }

    fun audit(): JsonNode = call("GET", "/internal/audit", key = null, base = World.ledgerUrl).body
}
