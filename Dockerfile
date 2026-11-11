# All of Ledgerline's images, from one build:
#   docker build --target ledger    -t ledgerline-ledger .
#   docker build --target payments  -t ledgerline-payments .
#   docker build --target otherbank -t ledgerline-otherbank .
#   docker build --target checker   -t ledgerline-checker .
# Ubuntu, not Alpine: the gRPC code generator is a glibc binary.
FROM gradle:9.8.2-jdk25-noble AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/home/gradle/.gradle gradle --no-daemon -q -x test \
      :ledger:bootJar :payments:bootJar :otherbank:installDist :checker:installDist \
    && mkdir /out \
    && cp ledger/build/libs/ledger.jar payments/build/libs/payments.jar /out/ \
    && cp -r otherbank/build/install/otherbank /out/otherbank \
    && cp -r checker/build/install/checker /out/checker

FROM eclipse-temurin:25.0.5_8-jre-alpine AS base
RUN addgroup -S -g 10001 ledgerline && adduser -S -u 10001 -G ledgerline ledgerline
USER 10001:10001
# Kafka's lz4 library loads native code and uses sun.misc.Unsafe: allowed, without Java 25's warnings.
ENV JDK_JAVA_OPTIONS="--enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow"

# The Spring Boot services start from an ahead-of-time cache (Java 25, JEP 483/514/515): classes already
# loaded and linked, and method profiles, from a training run of the service that starts it and exits
# (spring.context.exit=onRefresh; no database needed). A service killed mid-payment is back sooner.
# The jar is extracted first, as the cache needs a plain classpath. Serial GC, as on one CPU anyway, and
# the same in training and in use. Sized by the container's memory limit; virtual threads carry requests.
FROM base AS spring
USER root
RUN mkdir /app && chown 10001:10001 /app
USER 10001:10001
WORKDIR /app

FROM spring AS ledger
COPY --from=build --chown=10001:10001 /out/ledger.jar /tmp/ledger.jar
RUN java -Djarmode=tools -jar /tmp/ledger.jar extract --destination /app/ledger && rm /tmp/ledger.jar \
    && java -XX:+UseSerialGC -XX:AOTCacheOutput=/app/ledger/app.aot -Dspring.context.exit=onRefresh \
         -Dspring.flyway.enabled=false -jar /app/ledger/ledger.jar > /dev/null
EXPOSE 8080
ENTRYPOINT ["java", "-XX:+UseSerialGC", "-XX:AOTCache=/app/ledger/app.aot", "-XX:MaxRAMPercentage=70", "-XX:+ExitOnOutOfMemoryError", "-Djava.io.tmpdir=/tmp", "-jar", "/app/ledger/ledger.jar"]

FROM spring AS payments
COPY --from=build --chown=10001:10001 /out/payments.jar /tmp/payments.jar
RUN java -Djarmode=tools -jar /tmp/payments.jar extract --destination /app/payments && rm /tmp/payments.jar \
    && java -XX:+UseSerialGC -XX:AOTCacheOutput=/app/payments/app.aot -Dspring.context.exit=onRefresh \
         -Dspring.flyway.enabled=false -jar /app/payments/payments.jar > /dev/null
EXPOSE 8080
ENTRYPOINT ["java", "-XX:+UseSerialGC", "-XX:AOTCache=/app/payments/app.aot", "-XX:MaxRAMPercentage=70", "-XX:+ExitOnOutOfMemoryError", "-Djava.io.tmpdir=/tmp", "-jar", "/app/payments/payments.jar"]

FROM base AS otherbank
COPY --from=build /out/otherbank /app
# Its journal's directory, owned by it (a volume mounted here starts with this owner).
USER root
RUN mkdir /data && chown 10001:10001 /data
USER 10001:10001
ENV JAVA_OPTS="-XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8090
ENTRYPOINT ["/app/bin/otherbank"]

FROM base AS checker
COPY --from=build /out/checker /app
ENTRYPOINT ["/app/bin/checker"]
