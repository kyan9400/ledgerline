package dev.ledgerline.payments

import com.sun.net.httpserver.HttpServer
import dev.ledgerline.api.v1.AuthoriseRequest
import dev.ledgerline.api.v1.CaptureRequest
import dev.ledgerline.api.v1.GetPaymentRequest
import dev.ledgerline.api.v1.ListPaymentsRequest
import dev.ledgerline.api.v1.MerchantsGrpc
import dev.ledgerline.api.v1.PaymentEvent
import dev.ledgerline.api.v1.RefundRequest
import dev.ledgerline.api.v1.WatchRequest
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.stub.MetadataUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.beans.factory.annotation.Autowired
import java.net.InetSocketAddress
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class MerchantApiTest : PaymentsTest() {
    @Autowired
    lateinit var grpc: GrpcServer

    /** A merchant's webhook endpoint: refuses the first try of every event, then takes it. */
    class Receiver {
        data class Got(val eventId: String, val signature: String, val body: String)

        val got = CopyOnWriteArrayList<Got>()
        private val tries = ConcurrentHashMap<String, Int>()
        val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 50).apply {
            executor = Executors.newVirtualThreadPerTaskExecutor()
            createContext("/hooks") { ex ->
                val id = ex.requestHeaders.getFirst("Ledgerline-Event-Id")
                val body = ex.requestBody.readAllBytes().decodeToString()
                val n = tries.merge(id, 1, Int::plus)!!
                if (n == 1) {
                    ex.sendResponseHeaders(503, -1)
                } else {
                    got += Got(id, ex.requestHeaders.getFirst("Ledgerline-Signature"), body)
                    ex.sendResponseHeaders(204, -1)
                }
                ex.close()
            }
            start()
        }
        val url get() = "http://127.0.0.1:${server.address.port}/hooks"
    }

    private val receiver = Receiver()
    private val channels = mutableListOf<ManagedChannel>()

    @AfterEach
    fun close() {
        channels.forEach { it.shutdownNow() }
        receiver.server.stop(0)
    }

    private fun merchant(name: String, webhooks: Boolean = true): Pair<UUID, Map<String, String>> {
        val r = call("POST", "/internal/merchants", mapOf("name" to name, "webhookUrl" to if (webhooks) receiver.url else null), key = null)
        assertEquals(201, r.status, r.raw)
        return UUID.fromString(r.body["account"].asString()) to mapOf(
            "apiKey" to r.body["apiKey"].asString(), "secret" to r.body["webhookSecret"].asString(), "id" to r.body["id"].asString(),
        )
    }

    /** A stub that sends the API key, and this idempotency key (if any), with every call; the answer's headers go to `headersOut`. */
    private fun stub(apiKey: String, idempotencyKey: String? = UUID.randomUUID().toString(), headersOut: AtomicReference<Metadata>? = null): MerchantsGrpc.MerchantsBlockingStub {
        val channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpc.boundPort).usePlaintext().build().also { channels += it }
        val headers = Metadata().apply {
            put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer $apiKey")
            idempotencyKey?.let { put(Metadata.Key.of("idempotency-key", Metadata.ASCII_STRING_MARSHALLER), it) }
        }
        var s = MerchantsGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
        if (headersOut != null) s = s.withInterceptors(MetadataUtils.newCaptureMetadataInterceptor(headersOut, AtomicReference()))
        return s
    }

    private fun replayed(h: AtomicReference<Metadata>) = h.get().get(Metadata.Key.of("idempotent-replayed", Metadata.ASCII_STRING_MARSHALLER))

    @Test
    fun `a merchant authorises, captures and refunds over gRPC, and the money lands in its own account`() {
        val (shop, keys) = merchant("Corner Shop")
        val ann = open("ann", funded = 100_000)
        val api = { stub(keys.getValue("apiKey")) }
        val p = api().authorise(AuthoriseRequest.newBuilder().setCustomer(ann.toString()).setAmount(30_000).setDescription("tea").build())
        assertEquals("authorised", p.state)
        assertEquals(70_000, balance(ann))
        val c = api().capture(CaptureRequest.newBuilder().setPaymentId(p.id).setAmount(25_000).build())
        assertEquals("captured", c.state)
        assertEquals(25_000, balance(shop))
        val r = api().refundPayment(RefundRequest.newBuilder().setPaymentId(p.id).setAmount(5_000).build())
        assertEquals("refunded", r.state)
        assertEquals(80_000, balance(ann))
        assertEquals(20_000, balance(shop))
        assertEquals(5_000, api().getPayment(GetPaymentRequest.newBuilder().setPaymentId(p.id).build()).refunded)
    }

    @Test
    fun `a merchant sees only its own payments, and its idempotency keys are its own`() {
        val (_, a) = merchant("Shop A", webhooks = false)
        val (_, b) = merchant("Shop B", webhooks = false)
        val bob = open("bob", funded = 100_000)
        val req = AuthoriseRequest.newBuilder().setCustomer(bob.toString()).setAmount(1_000).build()

        val wrong = assertFailsWith<StatusRuntimeException> { stub("ll_sk_nonsense").authorise(req) }
        assertEquals(Status.Code.UNAUTHENTICATED, wrong.status.code)

        val noKey = assertFailsWith<StatusRuntimeException> { stub(a.getValue("apiKey"), idempotencyKey = null).authorise(req) }
        assertEquals(Status.Code.INVALID_ARGUMENT, noKey.status.code)
        assertEquals("idempotency_key_required", noKey.trailers!!.get(Metadata.Key.of("ledgerline-error-code", Metadata.ASCII_STRING_MARSHALLER)))

        val h = AtomicReference<Metadata>()
        val first = stub(a.getValue("apiKey"), "order-1", h).authorise(req)
        assertEquals("false", replayed(h))
        val again = stub(a.getValue("apiKey"), "order-1", h).authorise(req)
        assertEquals("true", replayed(h))
        assertEquals(first.id, again.id)
        val other = stub(b.getValue("apiKey"), "order-1").authorise(req) // the same key, another merchant: another payment
        assertTrue(other.id != first.id)
        assertEquals(98_000, balance(bob))

        val reused = assertFailsWith<StatusRuntimeException> {
            stub(a.getValue("apiKey"), "order-1").authorise(AuthoriseRequest.newBuilder().setCustomer(bob.toString()).setAmount(2_000).build())
        }
        assertEquals(Status.Code.FAILED_PRECONDITION, reused.status.code)

        val theirs = assertFailsWith<StatusRuntimeException> { stub(b.getValue("apiKey")).getPayment(GetPaymentRequest.newBuilder().setPaymentId(first.id).build()) }
        assertEquals(Status.Code.NOT_FOUND, theirs.status.code)
        val capture = assertFailsWith<StatusRuntimeException> { stub(b.getValue("apiKey")).capture(CaptureRequest.newBuilder().setPaymentId(first.id).build()) }
        assertEquals(Status.Code.NOT_FOUND, capture.status.code)

        val page1 = stub(a.getValue("apiKey")).listPayments(ListPaymentsRequest.newBuilder().setPageSize(1).build())
        assertEquals(listOf(first.id), page1.paymentsList.map { it.id })
        assertTrue(page1.nextPageToken.isEmpty(), "A has one payment")
    }

    @Test
    fun `every change is a signed webhook, delivered even when the first try is refused`() {
        val (_, keys) = merchant("Hook Shop")
        val cat = open("cat", funded = 100_000)
        val api = { stub(keys.getValue("apiKey")) }
        val p = api().authorise(AuthoriseRequest.newBuilder().setCustomer(cat.toString()).setAmount(10_000).build())
        api().capture(CaptureRequest.newBuilder().setPaymentId(p.id).build())
        val r = api().refundPayment(RefundRequest.newBuilder().setPaymentId(p.id).setAmount(4_000).build())

        val expected = setOf(
            "payment-${p.id}:1", "payment-${p.id}:2", "payment-${p.id}:3", "payment-${p.id}:4", "refund-${r.id}:1", "refund-${r.id}:2",
        )
        eventually(Duration.ofSeconds(30), "all six webhooks arrive (each after one refused try)") {
            receiver.got.map { it.eventId }.toSet().containsAll(expected)
        }
        for (g in receiver.got.filter { it.eventId in expected }) {
            val parts = g.signature.split(',').associate { it.substringBefore('=') to it.substringAfter('=') }
            assertEquals(Webhooks.sign(keys.getValue("secret"), parts.getValue("t").toLong(), g.body), parts.getValue("v1"), "signed with the merchant's secret")
            assertTrue(g.body.contains("\"id\":\"${g.eventId}\""))
        }
        val types = receiver.got.filter { it.eventId in expected }.map { json.readTree(it.body)["type"].asString() }.toSet()
        assertEquals(setOf("payment.authorising", "payment.authorised", "payment.capturing", "payment.captured", "refund.refunding", "refund.refunded"), types)
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "LEDGERLINE_TEST_KAFKA", matches = ".+")
    fun `a merchant watches its payments change, live, and only its own`() {
        val (_, keys) = merchant("Watching Shop", webhooks = false)
        val (_, other) = merchant("Other Shop", webhooks = false)
        val dan = open("dan", funded = 100_000)
        val events = CopyOnWriteArrayList<PaymentEvent>()
        val channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpc.boundPort).usePlaintext().build().also { channels += it }
        val headers = Metadata().apply { put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer ${keys.getValue("apiKey")}") }
        val watcher = MerchantsGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
        val reader = Executors.newVirtualThreadPerTaskExecutor()
        reader.execute {
            try {
                watcher.watchPayments(WatchRequest.getDefaultInstance()).forEach { events += it }
            } catch (_: StatusRuntimeException) {
            }
        }
        Thread.sleep(2_000) // the stream starts from "now"
        val mine = stub(keys.getValue("apiKey")).authorise(AuthoriseRequest.newBuilder().setCustomer(dan.toString()).setAmount(1_000).build())
        stub(other.getValue("apiKey")).authorise(AuthoriseRequest.newBuilder().setCustomer(dan.toString()).setAmount(1_000).build())
        stub(keys.getValue("apiKey")).capture(CaptureRequest.newBuilder().setPaymentId(mine.id).build())
        eventually(Duration.ofSeconds(30), "four events: authorising, authorised, capturing, captured") { events.size >= 4 }
        assertTrue(events.all { it.payment.id == mine.id }, "only its own payments")
        assertEquals(listOf(1, 2, 3, 4), events.map { it.payment.version }.distinct().sorted())
        assertEquals("captured", events.maxBy { it.payment.version }.payment.state)
        channel.shutdownNow()
        reader.shutdown()
        reader.awaitTermination(10, TimeUnit.SECONDS)
    }
}
