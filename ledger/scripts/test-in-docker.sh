#!/bin/sh
# Builds and tests the ledger in Docker against a throwaway PostgreSQL, then removes it.
#   ledger/scripts/test-in-docker.sh [gradle args]      (default: build)
# The Gradle cache lives in the volume ledgerline-gradle.
set -e
cd "$(dirname "$0")/.."
[ $# -eq 0 ] && set -- build
net=ledgerline-test-$$
pg=ledgerline-test-pg-$$
cleanup() { docker rm -f "$pg" >/dev/null 2>&1 || true; docker network rm "$net" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker network create "$net" >/dev/null
docker run -d --name "$pg" --network "$net" -e POSTGRES_DB=ledger_test -e POSTGRES_USER=ledger \
  -e POSTGRES_PASSWORD=ledger --tmpfs /var/lib/postgresql postgres:18.6-alpine \
  -c max_connections=200 -c fsync=off >/dev/null
docker run --rm -v ledgerline-gradle:/cache gradle:9.8.2-jdk25-alpine chown "$(id -u):$(id -g)" /cache
until docker exec "$pg" pg_isready -U ledger -d ledger_test >/dev/null 2>&1; do sleep 1; done
docker run --rm --network "$net" -u "$(id -u):$(id -g)" -e GRADLE_USER_HOME=/cache -e HOME=/tmp \
  -v ledgerline-gradle:/cache -v "$PWD:/w" -w /w \
  -e LEDGER_TEST_DB_URL="jdbc:postgresql://$pg:5432/ledger_test" \
  gradle:9.8.2-jdk25-alpine gradle --no-daemon --console=plain "$@"
