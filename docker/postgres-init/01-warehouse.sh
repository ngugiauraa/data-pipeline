#!/bin/bash
# Runs once, when the Postgres volume is first created: makes the warehouse
# database and a user that can only reach it.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE USER warehouse WITH PASSWORD '${WAREHOUSE_PASSWORD}';
CREATE DATABASE warehouse OWNER warehouse;
REVOKE ALL ON DATABASE warehouse FROM PUBLIC;
SQL
