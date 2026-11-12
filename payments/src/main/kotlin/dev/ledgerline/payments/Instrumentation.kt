package dev.ledgerline.payments

import io.micrometer.common.KeyValues
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.java11.instrument.binder.jdk.DefaultHttpClientObservationConvention
import io.micrometer.java11.instrument.binder.jdk.HttpClientContext
import io.micrometer.java11.instrument.binder.jdk.MicrometerHttpClient
import io.micrometer.observation.ObservationRegistry
import java.net.http.HttpClient
import java.net.http.HttpRequest

/** Observes the payments service's calls to the services it depends on. */
class Instrumentation(private val meters: MeterRegistry, private val observations: ObservationRegistry) {

    /**
     * For an [HttpService] to [peer] (ledger, otherbank, fraud): each call a client span, its traceparent
     * sent along, and in `http.client.requests` with `peer.service` (which the service graph draws), its
     * path with ids replaced (`/v1/accounts/{id}`) so the metric's series stay few.
     */
    fun http(peer: String): (HttpClient) -> HttpClient = { client ->
        MicrometerHttpClient.instrumentationBuilder(client, meters)
            .observationRegistry(observations)
            .customObservationConvention(Peer(peer))
            .uriMapper { req: HttpRequest -> req.uri().path.replace(ID, "{id}") }
            .build()
    }

    private class Peer(private val peer: String) : DefaultHttpClientObservationConvention() {
        override fun getLowCardinalityKeyValues(context: HttpClientContext): KeyValues =
            super.getLowCardinalityKeyValues(context).and("peer.service", peer)
    }

    companion object {
        private val ID = Regex("""[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|(?<=/)\d+(?=/|$)""")
    }
}
