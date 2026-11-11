rootProject.name = "ledgerline"

// platform: the outbox and load shedding, shared by the services. api: the merchants' gRPC API (from proto/).
// ledger, payments: Spring Boot services. otherbank: a simulated bank to pay out to. checker: a CLI that
// rebuilds the books from Kafka and compares.
include("platform", "api", "ledger", "payments", "otherbank", "checker")
