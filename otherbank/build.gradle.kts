plugins {
    application
}

dependencies {
    implementation("tools.jackson.core:jackson-databind")
    implementation("tools.jackson.module:jackson-module-kotlin")
}

application {
    mainClass = "dev.ledgerline.otherbank.OtherBankKt"
}
