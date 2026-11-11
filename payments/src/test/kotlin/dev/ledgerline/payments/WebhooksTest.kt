package dev.ledgerline.payments

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class WebhooksTest {
    @Test
    fun `the signature is HMAC-SHA256 over the time and the body, exactly as the SDK checks it`() {
        assertEquals(
            "3a1acea683c891b811cf0db1e8214daa2d539e0dee34471d955b0a8d2f044450",
            Webhooks.sign("ll_whsec_test", 1_794_000_000, """{"id":"payment-1:2"}"""),
        )
    }
}
