package dev.ledgerline.payments

import com.google.protobuf.Timestamp
import dev.ledgerline.api.v1.AuthoriseRequest as AuthoriseRpc
import dev.ledgerline.api.v1.CaptureRequest as CaptureRpc
import dev.ledgerline.api.v1.GetPaymentRequest
import dev.ledgerline.api.v1.ListPaymentsRequest
import dev.ledgerline.api.v1.ListPaymentsResponse
import dev.ledgerline.api.v1.MerchantsGrpc
import dev.ledgerline.api.v1.PaymentEvent
import dev.ledgerline.api.v1.RefundRequest as RefundRpc
import dev.ledgerline.api.v1.VoidRequest
import dev.ledgerline.api.v1.WatchRequest
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.ForwardingServerCall
import io.grpc.Metadata
import io.grpc.Server
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.stub.ServerCallStreamObserver
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.binder.grpc.ObservationGrpcServerInterceptor
import io.micrometer.observation.ObservationRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import dev.ledgerline.api.v1.Payment as PaymentMsg
import dev.ledgerline.api.v1.Refund as RefundMsg

private val MERCHANT: Context.Key<Merchant> = Context.key("merchant")
private val IDEMPOTENCY: Context.Key<String?> = Context.key("idempotency-key")
private val REPLAYED: Context.Key<AtomicBoolean> = Context.key("replayed")
private val AUTHORIZATION = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)
private val IDEMPOTENCY_KEY = Metadata.Key.of("idempotency-key", Metadata.ASCII_STRING_MARSHALLER)
private val REPLAYED_HEADER = Metadata.Key.of("idempotent-replayed", Metadata.ASCII_STRING_MARSHALLER)
private val ERROR_CODE = Metadata.Key.of("ledgerline-error-code", Metadata.ASCII_STRING_MARSHALLER)

/**
 * The merchants' gRPC API (proto/ledgerline/v1/merchants.proto), on its own port. It makes the same
 * writes as the HTTP API (Writes): the same idempotency, the same steps. What it adds: the merchant,
 * from its API key, is the payee of everything it authorises and can see nothing else; its idempotency
 * keys are its own; and it can watch its payments change, live, from Kafka.
 */
class MerchantApi(
    private val payments: Payments,
    private val writes: Writes,
    private val merchants: Merchants,
    private val json: JsonMapper,
    private val kafka: String,
) : MerchantsGrpc.MerchantsImplBase() {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun authorise(req: AuthoriseRpc, out: StreamObserver<PaymentMsg>) = call(out) {
        val m = MERCHANT.get()
        val customer = uuid(req.customer, "customer")
        answer(writes.authorise(key(), AuthoriseRequest(customer, m.account, req.amount, req.description))) { payment(it) }
    }

    override fun capture(req: CaptureRpc, out: StreamObserver<PaymentMsg>) = call(out) {
        val id = own(req.paymentId)
        answer(writes.capture(key(), id, CaptureRequest(if (req.hasAmount()) req.amount else null))) { payment(it) }
    }

    override fun voidPayment(req: VoidRequest, out: StreamObserver<PaymentMsg>) = call(out) {
        val id = own(req.paymentId)
        answer(writes.void(key(), id)) { payment(it) }
    }

    override fun refundPayment(req: RefundRpc, out: StreamObserver<RefundMsg>) = call(out) {
        val id = own(req.paymentId)
        answer(writes.refund(key(), id, RefundRequest(req.amount))) { refund(it) }
    }

    override fun getPayment(req: GetPaymentRequest, out: StreamObserver<PaymentMsg>) = call(out) {
        toProto(payments.payment(own(req.paymentId)))
    }

    override fun listPayments(req: ListPaymentsRequest, out: StreamObserver<ListPaymentsResponse>) = call(out) {
        val size = if (req.pageSize <= 0) 20 else req.pageSize.coerceAtMost(100)
        val after = req.pageToken.takeIf { it.isNotEmpty() }?.let { token ->
            val (t, id) = String(Base64.getUrlDecoder().decode(token)).split('|').let { it[0] to it[1] }
            Instant.parse(t) to UUID.fromString(id)
        }
        val page = payments.ofMerchant(MERCHANT.get().account, size + 1, after)
        val shown = page.take(size)
        ListPaymentsResponse.newBuilder().addAllPayments(shown.map(::toProto)).apply {
            if (page.size > size) {
                val last = shown.last()
                nextPageToken = Base64.getUrlEncoder().withoutPadding().encodeToString("${last.createdAt}|${last.id}".toByteArray())
            }
        }.build()
    }

    /**
     * The merchant's events, from Kafka (payments.events). Not from the database: a cursor over the event
     * log's sequence would skip an event whose transaction committed after a later one's; Kafka's offsets
     * have no such gaps. The cursor is every partition's next offset.
     */
    override fun watchPayments(req: WatchRequest, out: StreamObserver<PaymentEvent>) {
        val m = MERCHANT.get()
        val call = out as ServerCallStreamObserver<PaymentEvent>
        if (kafka.isBlank()) return out.onError(Status.UNAVAILABLE.withDescription("no Kafka configured").asRuntimeException())
        val props = mapOf<String, Any>(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false)
        try {
            KafkaConsumer(props, StringDeserializer(), StringDeserializer()).use { consumer ->
                val parts = consumer.partitionsFor(PAYMENTS_TOPIC, Duration.ofSeconds(5)).map { TopicPartition(PAYMENTS_TOPIC, it.partition()) }
                consumer.assign(parts)
                val positions = HashMap<Int, Long>()
                if (req.from.isEmpty()) {
                    consumer.endOffsets(parts).forEach { (p, o) -> positions[p.partition()] = o }
                } else {
                    String(Base64.getUrlDecoder().decode(req.from)).split(',').filter { it.isNotEmpty() }.forEach {
                        val (p, o) = it.split(':')
                        positions[p.toInt()] = o.toLong()
                    }
                }
                parts.forEach { p -> positions[p.partition()]?.let { consumer.seek(p, it) } ?: consumer.seekToBeginning(listOf(p)) }
                while (!call.isCancelled) {
                    for (rec in consumer.poll(Duration.ofMillis(500))) {
                        positions[rec.partition()] = rec.offset() + 1
                        val e = json.readTree(rec.value())
                        val data = e["data"]
                        if (data?.get("merchant")?.asString() != m.account.toString()) continue
                        val cursor = Base64.getUrlEncoder().withoutPadding()
                            .encodeToString(positions.entries.sortedBy { it.key }.joinToString(",") { "${it.key}:${it.value}" }.toByteArray())
                        val event = PaymentEvent.newBuilder().setId(e["id"].asString()).setType(e["type"].asString()).setCursor(cursor)
                            .setRecordedAt(ts(Instant.parse(e["recordedAt"].asString())))
                        if (e["type"].asString().startsWith("refund.")) event.refund = refund(data.toString()) else event.payment = payment(data.toString())
                        if (call.isCancelled) return
                        out.onNext(event.build())
                    }
                }
            }
        } catch (e: Exception) {
            if (!call.isCancelled) {
                log.debug("watch: {}", e.toString())
                out.onError(Status.UNAVAILABLE.withDescription("event stream: ${e.message}").asRuntimeException())
            }
            return
        }
        if (!call.isCancelled) out.onCompleted()
    }

    // --- helpers -------------------------------------------------------------------------------------

    private fun key(): String {
        val k = IDEMPOTENCY.get() ?: throw PaymentsException(400, "idempotency_key_required", "send an idempotency-key with every write")
        // The merchant's own: two merchants using the key "order-1" don't collide.
        return "m:${MERCHANT.get().id}:$k"
    }

    /** The payment, if it is this merchant's; anyone else's is not found (not "forbidden": that would say it exists). */
    private fun own(id: String): UUID {
        val pid = uuid(id, "payment_id")
        val p = try {
            payments.payment(pid)
        } catch (_: NotFound) {
            null
        }
        if (p == null || p.merchant != MERCHANT.get().account) throw NotFound("payment $pid")
        return pid
    }

    private fun uuid(s: String, field: String): UUID = try {
        UUID.fromString(s)
    } catch (_: IllegalArgumentException) {
        throw Invalid("$field is not an id")
    }

    /** An answer from Writes: the resource, or its refusal as a status. */
    private fun <T> answer(a: Answer, convert: (String) -> T): T {
        if (a.replayed) REPLAYED.get()?.set(true)
        if (a.status >= 400) {
            val p = json.readTree(a.body)
            throw PaymentsException(a.status, p["code"].asString(), p["detail"]?.asString() ?: "")
        }
        return convert(a.body)
    }

    private fun payment(body: String) = toProto(json.readValue(body, Payment::class.java))

    private fun refund(body: String) = json.readValue(body, Refund::class.java).let { r ->
        RefundMsg.newBuilder().setId(r.id.toString()).setPaymentId(r.payment.toString()).setState(r.state).setAmount(r.amount)
            .setFailureReason(r.failureReason ?: "").setVersion(r.version).setCreatedAt(ts(r.createdAt)).setUpdatedAt(ts(r.updatedAt)).build()
    }

    private fun toProto(p: Payment): PaymentMsg = PaymentMsg.newBuilder()
        .setId(p.id.toString()).setState(p.state).setCustomer(p.customer.toString()).setCurrency(p.currency)
        .setAmount(p.amount).setCaptured(p.captured).setRefunded(p.refunded).setDeclineReason(p.declineReason ?: "")
        .setVoidReason(p.voidReason ?: "").setDescription(p.description).setVersion(p.version)
        .setCreatedAt(ts(p.createdAt)).setUpdatedAt(ts(p.updatedAt))
        .apply { p.fraudScore?.let { fraudScore = it } }
        .build()

    private fun ts(i: Instant): Timestamp = Timestamp.newBuilder().setSeconds(i.epochSecond).setNanos(i.nano).build()

    /** Runs a unary call: its result, or the error as a status with the stable code in a trailer. */
    private fun <T> call(out: StreamObserver<T>, work: () -> T) {
        val result = try {
            work()
        } catch (e: PaymentsException) {
            val trailers = Metadata().apply { put(ERROR_CODE, e.code) }
            val code = if (e.code == "idempotency_key_reused") Status.FAILED_PRECONDITION else status(e.status)
            return out.onError(code.withDescription(e.message).asRuntimeException(trailers))
        } catch (e: StatusRuntimeException) {
            return out.onError(e)
        } catch (e: Exception) {
            log.warn("merchant API: {}", e.toString())
            return out.onError(Status.UNAVAILABLE.withDescription("try again with the same idempotency-key").asRuntimeException())
        }
        out.onNext(result)
        out.onCompleted()
    }

    private fun status(http: Int): Status = when (http) {
        400, 422 -> Status.INVALID_ARGUMENT
        401 -> Status.UNAUTHENTICATED
        403 -> Status.PERMISSION_DENIED
        404 -> Status.NOT_FOUND
        409 -> Status.FAILED_PRECONDITION
        503 -> Status.UNAVAILABLE
        else -> Status.INTERNAL
    }

    companion object {
        /** Who is calling (its API key), and its idempotency key; the replayed flag goes back as a header. */
        fun authenticate(merchants: Merchants): ServerInterceptor = object : ServerInterceptor {
            override fun <Q, R> interceptCall(call: ServerCall<Q, R>, headers: Metadata, next: ServerCallHandler<Q, R>): ServerCall.Listener<Q> {
                val key = headers.get(AUTHORIZATION)?.removePrefix("Bearer ")?.trim()
                val merchant = key?.let { merchants.byApiKey(it) }
                if (merchant == null) {
                    call.close(Status.UNAUTHENTICATED.withDescription("send authorization: Bearer <your API key>"), Metadata())
                    return object : ServerCall.Listener<Q>() {}
                }
                val replayed = AtomicBoolean(false)
                val ctx = Context.current().withValue(MERCHANT, merchant).withValue(IDEMPOTENCY, headers.get(IDEMPOTENCY_KEY))
                    .withValue(REPLAYED, replayed)
                val withHeader = object : ForwardingServerCall.SimpleForwardingServerCall<Q, R>(call) {
                    override fun sendHeaders(h: Metadata) {
                        h.put(REPLAYED_HEADER, replayed.get().toString())
                        super.sendHeaders(h)
                    }
                }
                return Contexts.interceptCall(ctx, withHeader, headers, next)
            }
        }
    }
}

/** The gRPC server, started and stopped with the application. */
/** Each call is observed (ObservationGrpcServerInterceptor): a span continuing the caller's trace, and the grpc.server timer. */
class GrpcServer(
    private val port: Int,
    private val api: MerchantApi,
    private val merchants: Merchants,
    private val observations: ObservationRegistry = ObservationRegistry.NOOP,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private var server: Server? = null

    /** The port it listens on (with 0 configured, the one it was given). */
    val boundPort: Int get() = server?.port ?: port

    override fun start() {
        server = NettyServerBuilder.forPort(port)
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            // The last runs first: the observation wraps everything, the refusals included.
            .addService(ServerInterceptors.intercept(api, MerchantApi.authenticate(merchants), ObservationGrpcServerInterceptor(observations)))
            .build().start()
        log.info("merchants' gRPC API on port {}", server!!.port)
    }

    override fun stop() {
        server?.shutdown()?.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
    }

    override fun isRunning() = server?.isShutdown == false
}
