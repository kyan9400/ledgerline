package dev.ledgerline.payments

import org.springframework.jdbc.core.simple.JdbcClient
import java.security.MessageDigest
import java.security.SecureRandom
import java.sql.ResultSet
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

data class Merchant(val id: UUID, val name: String, val account: UUID, val webhookUrl: String?, val webhookSecret: String)

/** A new merchant's credentials: shown once, then only their hash is kept (the API key's). */
data class NewMerchant(val id: UUID, val name: String, val account: UUID, val apiKey: String, val webhookUrl: String?, val webhookSecret: String)

data class MerchantRequest(val name: String, val webhookUrl: String? = null)

/**
 * The merchants who use the gRPC API. Each has a merchant account in the ledger, an API key (kept as its
 * SHA-256, so a stolen database doesn't give the keys away) and a secret its webhooks are signed with.
 * Merchants don't change once made, so lookups are kept in memory.
 */
class Merchants(private val db: JdbcClient, private val ledger: LedgerOps) {
    private val byKey = ConcurrentHashMap<String, Merchant>()
    private val byAccount = ConcurrentHashMap<UUID, Merchant>()
    private val random = SecureRandom()

    fun create(req: MerchantRequest): NewMerchant {
        if (req.name.isBlank() || req.name.length > 100) throw Invalid("name is 1-100 characters")
        req.webhookUrl?.let { if (!Regex("^https?://[^\\s]{1,500}$").matches(it)) throw Invalid("webhookUrl is an http(s) URL") }
        val id = UUID.randomUUID()
        val code = "merchant:" + req.name.lowercase().replace(Regex("[^a-z0-9]+"), "-").trim('-').take(60) + ":" + id.toString().take(8)
        val account = when (val o = ledger.openAccount("payments:merchant:$id", code, "RUB", allowNegative = false, kind = "merchant")) {
            is Outcome.Done -> UUID.fromString(o.body["id"].asString())
            is Outcome.Refused -> throw Invalid("the ledger refused the account: ${o.code} ${o.detail}")
            is Outcome.Unavailable -> throw LedgerUnavailable(o.reason)
        }
        val apiKey = "ll_sk_" + token(32)
        val secret = "ll_whsec_" + token(32)
        db.sql(
            """INSERT INTO merchants (id, name, account, api_key_hash, webhook_url, webhook_secret)
               VALUES (:id, :name, :account, :hash, :url, :secret)""",
        ).param("id", id).param("name", req.name).param("account", account).param("hash", sha256(apiKey))
            .param("url", req.webhookUrl).param("secret", secret).update()
        return NewMerchant(id, req.name, account, apiKey, req.webhookUrl, secret)
    }

    fun byApiKey(apiKey: String): Merchant? {
        val hash = sha256(apiKey)
        return byKey[hash] ?: db.sql("SELECT * FROM merchants WHERE api_key_hash = :h").param("h", hash)
            .query { rs, _ -> merchantOf(rs) }.optional().orElse(null)?.also { byKey[hash] = it }
    }

    /** The merchant whose ledger account this is, if it is one of ours. */
    fun byAccount(account: UUID): Merchant? = byAccount[account] ?: db.sql("SELECT * FROM merchants WHERE account = :a").param("a", account)
        .query { rs, _ -> merchantOf(rs) }.optional().orElse(null)?.also { byAccount[account] = it }

    fun byId(id: UUID): Merchant = db.sql("SELECT * FROM merchants WHERE id = :id").param("id", id)
        .query { rs, _ -> merchantOf(rs) }.optional().orElseThrow { NotFound("merchant $id") }

    private fun merchantOf(rs: ResultSet) = Merchant(
        rs.getObject("id", UUID::class.java), rs.getString("name"), rs.getObject("account", UUID::class.java),
        rs.getString("webhook_url"), rs.getString("webhook_secret"),
    )

    private fun token(n: Int): String {
        val alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        return (1..n).map { alphabet[random.nextInt(alphabet.length)] }.joinToString("")
    }

    companion object {
        fun sha256(s: String) = MessageDigest.getInstance("SHA-256").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }
    }
}
