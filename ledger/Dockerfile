# The ledger service: built with Gradle, run on a JRE as a non-root user.
FROM gradle:9.8.2-jdk25-alpine AS build
WORKDIR /src
COPY settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
# Dependencies first, so a code change doesn't download them again.
RUN --mount=type=cache,target=/home/gradle/.gradle gradle --no-daemon -q dependencies > /dev/null
COPY src ./src
RUN --mount=type=cache,target=/home/gradle/.gradle gradle --no-daemon -q bootJar -x test \
    && mv build/libs/ledger-*.jar /ledger.jar

FROM eclipse-temurin:25.0.5_8-jre-alpine
RUN addgroup -S -g 10001 ledger && adduser -S -u 10001 -G ledger ledger
COPY --from=build /ledger.jar /app/ledger.jar
USER 10001:10001
EXPOSE 8080
# Sized by the container's memory limit; virtual threads carry the requests (application.yaml).
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-XX:+ExitOnOutOfMemoryError", "-Djava.io.tmpdir=/tmp", "-jar", "/app/ledger.jar"]
