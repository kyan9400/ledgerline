package dev.ledgerline.payments

import dev.ledgerline.platform.Traces
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.jdbc.core.simple.JdbcClient
import tools.jackson.databind.json.JsonMapper
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.Timestamp
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Webhooks: every change to a merchant's payment or refund, POSTed to the merchant.
 *
 * - **Queued with the change.** A delivery is inserted in the same database transaction as the change it
 *   describes, so it is sent if and only if the change commits.
 * - **Signed.** `Ledgerline-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "t.body", keyed by the
 *   merchant's secret>`. A receiver checks it, and that t is recent (a replayed old request fails).
 * - **At least once.** Anything but a 2xx in 10 s is tried again: after 1 s, doubling, at most 10 minutes
 *   apart, 12 times in all; then the delivery is kept as failed, and can be redelivered. A receiver skips
 *   an event id it has handled. Order isn't guaranteed: the event's version says which is newer.
 */
class Webhooks(
    private val db: JdbcClient,
    private val merchants: Merchants,
    private val json: JsonMapper,
    private val clock: Clock,
    meters: MeterRegistry,
    private val traces: Traces = Traces.NONE,
) {
    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val delivered = meters.counter("payments.webhooks.delivered")
    private val retried = meters.counter("payments.webhooks.retried")
    private val failed = meters.counter("payments.webhooks.failed")

    /** In the caller's transaction: the event, for the merchant whose payment it is (if it has a URL). */
    fun queue(merchantAccount: UUID, eventId: String, type: String, version: Int, dataJson: String, at: Instant) {
        val m = merchants.byAccount(merchantAccount) ?: return
        if (m.webhookUrl == null) return
        val payload = buildString(dataJson.length + 160) {
            append("{\"id\":").append(json.writeValueAsString(eventId))
            append(",\"type\":").append(json.writeValueAsString(type))
            append(",\"version\":").append(version)
            append(",\"created\":\"").append(at).append('"')
            append(",\"data\":").append(dataJson).append('}')
        }
        db.sql(
            """INSERT INTO webhook_deliveries (merchant_id, event_id, type, payload, next_attempt_at, trace_context)
               VALUES (:m, :e, :t, :p, :now, :tc) ON CONFLICT (merchant_id, event_id) DO NOTHING""",
        ).param("m", m.id).param("tc", traces.current()).param("e", eventId).param("t", type).param("p", payload).param("now", Timestamp.from(clock.instant())).update()
    }

    /** Due deliveries, leased for a minute (longer than a delivery can take). */
    fun claimDue(limit: Int): List<Due<Long>> = db.sql(
        """UPDATE webhook_deliveries SET next_attempt_at = :lease WHERE id IN (
             SELECT id FROM webhook_deliveries WHERE next_attempt_at <= :now ORDER BY next_attempt_at LIMIT :n FOR UPDATE SKIP LOCKED)
           RETURNING id, trace_context""",
    ).param("lease", Timestamp.from(clock.instant().plusSeconds(60))).param("now", Timestamp.from(clock.instant())).param("n", limit)
        .query { rs, _ -> Due(rs.getLong(1), rs.getString(2)) }.list()

    fun deliver(id: Long) {
        val d = db.sql("SELECT merchant_id, event_id, payload, attempts FROM webhook_deliveries WHERE id = :id AND state = 'pending'")
            .param("id", id).query { rs, _ -> Delivery(rs.getObject(1, UUID::class.java), rs.getString(2), rs.getString(3), rs.getInt(4)) }
            .optional().orElse(null) ?: return
        val m = merchants.byId(d.merchant)
        // null: the merchant took it.
        val error = if (m.webhookUrl == null) "no webhook URL" else send(m.webhookUrl, m.webhookSecret, id, d)
        val attempts = d.attempts + 1
        traces.tag("ledgerline.webhook.attempt", attempts.toString())
        traces.tag("ledgerline.webhook.outcome", error ?: "delivered")
        when {
            error == null -> {
                db.sql("UPDATE webhook_deliveries SET state = 'delivered', attempts = :a, delivered_at = now(), next_attempt_at = NULL, last_error = NULL WHERE id = :id")
                    .param("a", attempts).param("id", id).update()
                delivered.increment()
            }
            attempts >= MAX_ATTEMPTS -> {
                db.sql("UPDATE webhook_deliveries SET state = 'failed', attempts = :a, next_attempt_at = NULL, last_error = :e WHERE id = :id")
                    .param("a", attempts).param("e", error.take(500)).param("id", id).update()
                failed.increment()
            }
            else -> {
                val delay = Duration.ofSeconds((1L shl (attempts - 1)).coerceAtMost(600))
                db.sql("UPDATE webhook_deliveries SET attempts = :a, next_attempt_at = :next, last_error = :e WHERE id = :id")
                    .param("a", attempts).param("next", Timestamp.from(clock.instant().plus(delay))).param("e", error.take(500)).param("id", id).update()
                retried.increment()
            }
        }
    }

    /** Failed deliveries of a merchant, queued again: returns how many. */
    fun redeliver(merchant: UUID): Int = db.sql(
        "UPDATE webhook_deliveries SET state = 'pending', attempts = 0, next_attempt_at = now() WHERE merchant_id = :m AND state = 'failed'",
    ).param("m", merchant).update()

    /** A failure's description, or null when the merchant took it. */
    private fun send(url: String, secret: String, id: Long, d: Delivery): String? {
        val t = clock.instant().epochSecond
        val req = HttpRequest.newBuilder(URI(url)).timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .header("User-Agent", "Ledgerline-Webhooks/1")
            .header("Ledgerline-Event-Id", d.eventId)
            .header("Ledgerline-Delivery", id.toString())
            .header("Ledgerline-Signature", "t=$t,v1=${sign(secret, t, d.payload)}")
            .POST(HttpRequest.BodyPublishers.ofString(d.payload)).build()
        return try {
            val code = http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode()
            if (code in 200..299) null else "HTTP $code"
        } catch (e: IOException) {
            e.javaClass.simpleName + (e.message?.let { ": $it" } ?: "")
        } catch (e: IllegalArgumentException) {
            "bad URL: ${e.message}"
        }
    }

    private class Delivery(val merchant: UUID, val eventId: String, val payload: String, val attempts: Int)

    companion object {
        const val MAX_ATTEMPTS = 12

        fun sign(secret: String, timestamp: Long, payload: String): String {
            val mac = Mac.getInstance("HmacSHA256")
            mac.init(SecretKeySpec(secret.toByteArray(), "HmacSHA256"))
            return mac.doFinal("$timestamp.$payload".toByteArray()).joinToString("") { "%02x".format(it) }
        }
    }
}
