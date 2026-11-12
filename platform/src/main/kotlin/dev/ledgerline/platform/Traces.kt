package dev.ledgerline.platform

import io.micrometer.tracing.Span
import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator

/**
 * Carries a trace across the places a call stack doesn't reach: a row written now and acted on later,
 * perhaps by another process. The outbox, a saga's next step and a webhook delivery each store the trace
 * context of the moment they were written (a W3C `traceparent`), and whoever picks the row up continues
 * that trace. So a payment's trace runs from the merchant's request to the Kafka message and the webhook,
 * and through a step the worker finished after the service was killed.
 *
 * The sampling decision travels with it: a row from a trace that wasn't sampled continues unsampled.
 */
class Traces(private val tracer: Tracer, private val propagator: Propagator) {

    /** The current span's `traceparent`, or null outside any trace. */
    fun current(): String? {
        val context = tracer.currentTraceContext().context() ?: return null
        return headers(context)[TRACEPARENT]
    }

    /**
     * A started span that continues the trace in [traceparent] (a new trace without one). Not current.
     * [kind] null: an internal span.
     */
    fun start(traceparent: String?, name: String, kind: Span.Kind?): Span {
        val builder = if (traceparent == null) {
            tracer.spanBuilder().setNoParent()
        } else {
            propagator.extract(traceparent) { tp, key -> if (key == TRACEPARENT) tp else null }
        }
        val named = builder.name(name)
        return (if (kind != null) named.kind(kind) else named).start()
    }

    /** Runs [block] in a span continuing [traceparent], current while it runs; a throw marks it failed. */
    fun <T> within(traceparent: String?, name: String, kind: Span.Kind?, tags: Map<String, String> = emptyMap(), block: () -> T): T {
        val span = start(traceparent, name, kind)
        tags.forEach { (k, v) -> span.tag(k, v) }
        try {
            return tracer.withSpan(span).use { block() }
        } catch (e: Throwable) {
            span.error(e)
            throw e
        } finally {
            span.end()
        }
    }

    /** Tags the current span, if there is one. */
    fun tag(key: String, value: String) {
        tracer.currentSpan()?.tag(key, value)
    }

    /** The propagation headers for [span] (for a Kafka record). */
    fun headers(span: Span): Map<String, String> = headers(span.context())

    private fun headers(context: io.micrometer.tracing.TraceContext): Map<String, String> {
        val carrier = HashMap<String, String>(4)
        propagator.inject(context, carrier) { c, k, v -> c?.put(k, v) }
        return carrier
    }

    companion object {
        const val TRACEPARENT = "traceparent"

        /** No tracing (the tests, or a service without it). */
        val NONE = Traces(Tracer.NOOP, Propagator.NOOP)
    }
}
