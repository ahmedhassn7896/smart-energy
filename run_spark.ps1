# ==============================================================================
# Smart Energy — Spark Structured Streaming Job Launcher
# ==============================================================================
# FIX NOTES (2026-09-16):
#   1. clickhouse-jdbc REMOVED from --packages (Maven DNS unreachable inside
#      container; not needed — writes use urllib.request HTTP directly).
#   2. --conf spark.executorEnv.* required to pass ClickHouse creds to workers.
#   3. Use -CleanCheckpoint if the previous job was killed mid-write.
# ==============================================================================

param(
    [switch]$CleanCheckpoint
)

$ErrorActionPreference = "Stop"

if ($CleanCheckpoint) {
    Write-Host "[INFO] Deleting stale Spark checkpoint..." -ForegroundColor Yellow
    docker exec ntibigdata-spark-master bash -c "rm -rf /opt/spark/work/checkpoints/kafka_clickhouse && echo Checkpoint cleared"
}

$CH_HOST     = "clickhouse"
$CH_PORT     = "8123"
$CH_USER     = "default"
$CH_PASSWORD = "clickhouse"
$CH_DB       = "analytics"

Write-Host "[INFO] Submitting Spark streaming job..." -ForegroundColor Cyan

docker exec `
    -e CLICKHOUSE_HOST=$CH_HOST `
    -e CLICKHOUSE_HTTP_PORT=$CH_PORT `
    -e CLICKHOUSE_USER=$CH_USER `
    -e CLICKHOUSE_PASSWORD=$CH_PASSWORD `
    -e CLICKHOUSE_DB=$CH_DB `
    ntibigdata-spark-master `
    /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf "spark.executorEnv.CLICKHOUSE_HOST=$CH_HOST" `
    --conf "spark.executorEnv.CLICKHOUSE_HTTP_PORT=$CH_PORT" `
    --conf "spark.executorEnv.CLICKHOUSE_USER=$CH_USER" `
    --conf "spark.executorEnv.CLICKHOUSE_PASSWORD=$CH_PASSWORD" `
    --conf "spark.executorEnv.CLICKHOUSE_DB=$CH_DB" `
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1" `
    /opt/spark-apps/kafka_to_clickhouse.py
