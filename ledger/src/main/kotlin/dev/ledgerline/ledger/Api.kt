package dev.ledgerline.ledger

import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RestControllerAdvice
import java.util.UUID

private const val KEY = "Idempotency-Key"

data class ReverseRequest(val description: String = "")

/**
 * The ledger's HTTP API. Every write needs an Idempotency-Key header: a retry with the same key gets the
 * same answer (`Idempotent-Replayed: true`), whatever the first was.
 */
@RestController
@RequestMapping("/v1")
class Api(private val ledger: Ledger, private val idem: Idempotency) {

    @PostMapping("/accounts")
    fun open(@RequestHeader(KEY, required = false) key: String?, @RequestBody body: OpenAccount) =
        reply(idem.run(key, "open-account", body) { ledger.openAccountIn(body) })

    @GetMapping("/accounts/{id}")
    fun account(@PathVariable id: UUID) = ledger.account(id)

    @GetMapping("/accounts")
    fun accountByCode(@RequestParam code: String) = ledger.accountByCode(code)

    @GetMapping("/accounts/{id}/entries")
    fun statement(
        @PathVariable id: UUID,
        @RequestParam(defaultValue = "50") limit: Int,
        @RequestParam(required = false) before: Int?,
    ) = ledger.statement(id, limit, before)

    @PostMapping("/transfers")
    fun transfer(@RequestHeader(KEY, required = false) key: String?, @RequestBody body: TransferRequest) =
        reply(idem.run(key, "transfer", body) { ledger.transferIn(body) })

    @PostMapping("/transactions")
    fun post(@RequestHeader(KEY, required = false) key: String?, @RequestBody body: PostingRequest) =
        reply(idem.run(key, "posting", body) { ledger.postIn(body) })

    @GetMapping("/transactions/{id}")
    fun transaction(@PathVariable id: UUID) = ledger.transaction(id)

    @PostMapping("/transactions/{id}/reversal")
    fun reverse(
        @RequestHeader(KEY, required = false) key: String?,
        @PathVariable id: UUID,
        @RequestBody(required = false) body: ReverseRequest?,
    ) = (body ?: ReverseRequest()).let { b ->
        reply(idem.run(key, "reverse $id", b) { ledger.reverseIn(id, b.description) })
    }

    private fun reply(s: Stored): ResponseEntity<Any> = ResponseEntity.status(s.status)
        .contentType(if (s.status >= 400) MediaType.APPLICATION_PROBLEM_JSON else MediaType.APPLICATION_JSON)
        .header("Idempotent-Replayed", s.replayed.toString())
        .body(s.body)
}

/** Internal: not routed from outside (the proxy only forwards /v1). */
@RestController
@RequestMapping("/internal")
class Internal(private val audit: Audit) {
    @GetMapping("/audit")
    fun audit(): ResponseEntity<AuditReport> = audit.run().let {
        ResponseEntity.status(if (it.ok) HttpStatus.OK else HttpStatus.INTERNAL_SERVER_ERROR).body(it)
    }
}

@RestControllerAdvice
@Suppress("UNUSED_PARAMETER")
class Errors {
    @ExceptionHandler(LedgerException::class)
    fun ledger(e: LedgerException): ResponseEntity<Map<String, Any?>> =
        ResponseEntity.status(e.status).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(problem(e))

    @ExceptionHandler(HttpMessageNotReadableException::class)
    fun unreadable(e: HttpMessageNotReadableException): ResponseEntity<Map<String, Any?>> =
        ledger(Invalid("the request body isn't valid JSON for this operation"))

    @ExceptionHandler(ArithmeticException::class)
    fun overflow(e: ArithmeticException): ResponseEntity<Map<String, Any?>> =
        ledger(Invalid("amounts too large: they would overflow a 64-bit balance"))
}
