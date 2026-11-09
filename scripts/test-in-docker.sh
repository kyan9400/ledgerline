#!/bin/sh
# Builds and tests everything in Docker against a throwaway PostgreSQL and Kafka, then removes them.
#   scripts/test-in-docker.sh [gradle args]      (default: build)
# The Gradle cache lives in the volume ledgerline-gradle.
set -e
cd "$(dirname "$0")/.."
[ $# -eq 0 ] && set -- build
net=ledgerline-test-$$
pg=ledgerline-test-pg-$$
kafka=ledgerline-test-kafka-$$
cleanup() {
  docker rm -f "$pg" "$kafka" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create "$net" >/dev/null
docker run -d --name "$pg" --network "$net" -e POSTGRES_DB=ledger_test -e POSTGRES_USER=ledger \
  -e POSTGRES_PASSWORD=ledger --tmpfs /var/lib/postgresql postgres:18.6-alpine \
  -c max_connections=300 -c fsync=off >/dev/null
docker run -d --name "$kafka" --network "$net" --tmpfs /var/lib/kafka/data \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://$kafka:9092" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_HEAP_OPTS="-Xmx512m -Xms512m" apache/kafka:4.4.0 >/dev/null
docker run --rm -v ledgerline-gradle:/cache gradle:9.8.2-jdk25-alpine chown "$(id -u):$(id -g)" /cache
# (pg_isready can answer during the image's first-start initialisation, before the restart: so retry.)
until docker exec -e PGPASSWORD=ledger "$pg" psql -q -h 127.0.0.1 -U ledger -d ledger_test -c 'CREATE DATABASE payments_test' >/dev/null 2>&1; do sleep 1; done
docker run --rm --network "$net" -u "$(id -u):$(id -g)" -e GRADLE_USER_HOME=/cache -e HOME=/tmp \
  -v ledgerline-gradle:/cache -v "$PWD:/w" -w /w \
  -e LEDGERLINE_TEST_DB="jdbc:postgresql://$pg:5432" \
  -e LEDGERLINE_TEST_KAFKA="$kafka:9092" \
  gradle:9.8.2-jdk25-alpine gradle --no-daemon --console=plain "$@"
