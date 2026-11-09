# All of Ledgerline's images, from one build:
#   docker build --target ledger    -t ledgerline-ledger .
#   docker build --target payments  -t ledgerline-payments .
#   docker build --target otherbank -t ledgerline-otherbank .
#   docker build --target checker   -t ledgerline-checker .
FROM gradle:9.8.2-jdk25-alpine AS build
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

# Sized by the container's memory limit; virtual threads carry the requests.
FROM base AS ledger
COPY --from=build /out/ledger.jar /app/ledger.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-XX:+ExitOnOutOfMemoryError", "-Djava.io.tmpdir=/tmp", "-jar", "/app/ledger.jar"]

FROM base AS payments
COPY --from=build /out/payments.jar /app/payments.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-XX:+ExitOnOutOfMemoryError", "-Djava.io.tmpdir=/tmp", "-jar", "/app/payments.jar"]

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
