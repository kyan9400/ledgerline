plugins {
    application
}

dependencies {
    implementation("org.apache.kafka:kafka-clients")
    implementation("org.postgresql:postgresql")
    implementation("tools.jackson.core:jackson-databind")
    runtimeOnly("org.slf4j:slf4j-nop")
}

application {
    mainClass = "dev.ledgerline.checker.CheckerKt"
}
