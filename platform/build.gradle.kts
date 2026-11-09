plugins {
    kotlin("plugin.spring")
}

dependencies {
    api("org.apache.kafka:kafka-clients")
    implementation("org.springframework.boot:spring-boot-autoconfigure")
    implementation("org.springframework:spring-jdbc")
    implementation("org.springframework:spring-webmvc")
    implementation("jakarta.servlet:jakarta.servlet-api")
    implementation("tools.jackson.core:jackson-databind")
    implementation("io.micrometer:micrometer-core")
    implementation("org.slf4j:slf4j-api")

    testImplementation("org.springframework:spring-test")
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc")
    testRuntimeOnly("org.postgresql:postgresql")
}
