#!/usr/bin/env bash
# ==============================================================================
# ClickHouse Database Initialization
# Creates the requested 'analytics' database without any project data tables.
# ==============================================================================

set -e

CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-clickhouse_password}"
TARGET_DB="${CLICKHOUSE_DB:-analytics}"

echo "[ClickHouse Init] Initializing database '${TARGET_DB}'..."

clickhouse-client -n \
    --user "${CLICKHOUSE_USER}" \
    --password "${CLICKHOUSE_PASSWORD}" \
    --query "CREATE DATABASE IF NOT EXISTS ${TARGET_DB};"

echo "[ClickHouse Init] Database '${TARGET_DB}' is ready."
