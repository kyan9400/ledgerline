#!/bin/sh
# Runs once, when PostgreSQL first initialises its volume: the payments service's own role and database.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v pw="$PAYMENTS_DB_PASSWORD" <<'SQL'
CREATE ROLE payments LOGIN PASSWORD :'pw';
CREATE DATABASE payments OWNER payments;
REVOKE ALL ON DATABASE ledger FROM PUBLIC;
SQL
