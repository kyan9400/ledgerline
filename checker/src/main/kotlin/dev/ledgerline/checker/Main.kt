package dev.ledgerline.checker

import tools.jackson.databind.json.JsonMapper
import kotlin.system.exitProcess

/**
 * Ledgerline's tools, configured by environment variables (see each command):
 *
 * ```
 * checker check                  rebuild the books from Kafka and compare (the default)
 * checker republish <service>    fill Kafka again from a service's event log (ledger or payments)
 * checker workload <log.json>    a client that keeps paying, and logs every request and answer
 * checker verify <log.json>      prove from that log, the ledger and both databases that nothing was
 *                                lost or done twice
 * ```
 *
 * Each prints a JSON report and exits 0 when everything holds, 1 when something doesn't.
 */
fun main(args: Array<String>) {
    val env = System.getenv()
    val (report, ok) = when (args.firstOrNull() ?: "check") {
        "check" -> check(env)
        "republish" -> republish(env, args.getOrNull(1) ?: usage())
        "workload" -> workload(env, args.getOrNull(1) ?: usage())
        "verify" -> verify(env, args.getOrNull(1) ?: usage())
        else -> usage()
    }
    println(JsonMapper.builder().build().writerWithDefaultPrettyPrinter().writeValueAsString(report))
    exitProcess(if (ok) 0 else 1)
}

private fun usage(): Nothing {
    System.err.println("usage: checker check | republish <ledger|payments> | workload <log.json> | verify <log.json>")
    exitProcess(2)
}
