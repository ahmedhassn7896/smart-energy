import os
import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ---------------------------------------------------------
# ClickHouse connection — read from environment variables
# Set these via docker-compose.yml or your .env file.
# ---------------------------------------------------------
_CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
_CH_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
_CH_USER = os.getenv("CLICKHOUSE_USER", "default")
_CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
_CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", os.getenv("CLICKHOUSE_DB", "analytics"))

print("=== Starting Smart Energy Spark-to-ClickHouse Streaming Job ===", flush=True)

spark = (
    SparkSession.builder
    .appName("SmartEnergyKafkaToClickHouse")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# Kafka message schema
# ---------------------------------------------------------
schema = StructType([
    StructField("household_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("energy_consumption", DoubleType(), True),
])

# ---------------------------------------------------------
# Read from Kafka
# ---------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "smart_energy")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 100000)
    .load()
)

# ---------------------------------------------------------
# Parse JSON
# ---------------------------------------------------------
parsed = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)

# ---------------------------------------------------------
# Clean and transform
# ---------------------------------------------------------
clean = (
    parsed
    .filter(col("household_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("energy_consumption").isNotNull())
    .filter(col("energy_consumption") >= 0.0)
    .withColumn(
        "reading_datetime",
        to_timestamp(
            col("timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .filter(col("reading_datetime").isNotNull())
    .select(
        col("household_id").alias("lcl_id"),
        col("reading_datetime"),
        col("energy_consumption").alias("kwh")
    )
)

# ---------------------------------------------------------
# Write partition directly to ClickHouse via HTTP TabSeparated
# ---------------------------------------------------------
def write_partition(rows):
    """Write a Spark partition to ClickHouse in 10,000-row chunks.

    Uses foreachPartition so each executor writes directly — no data is
    collected into the driver. Chunks avoid building an unbounded list for
    large partitions.  Retries each chunk up to 3 times on failure.
    """
    import urllib.request
    import urllib.parse
    import time
    import os
    from datetime import datetime

    CHUNK_SIZE = 10000

    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    ch_db = os.getenv("CLICKHOUSE_DATABASE", os.getenv("CLICKHOUSE_DB", "analytics"))

    CLICKHOUSE_URL = (
        f"http://{ch_host}:{ch_port}/?"
        + urllib.parse.urlencode({
            "user": ch_user,
            "password": ch_password,
            "query": f"INSERT INTO {ch_db}.raw_smart_meter_events (lcl_id, reading_datetime, kwh) FORMAT TabSeparated"
        })
    )

    chunk = []

    def send_chunk(buffer):
        if not buffer:
            return
        payload = "".join(buffer).encode("utf-8")
        req = urllib.request.Request(
            CLICKHOUSE_URL,
            data=payload,
            method="POST",
            headers={"Content-Type": "text/tab-separated-values; charset=utf-8"}
        )
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    resp.read()
                return
            except Exception as ex:
                if attempt == max_retries - 1:
                    raise RuntimeError(f"ClickHouse HTTP insert failed after {max_retries} attempts: {ex}")
                time.sleep(1.0 * (attempt + 1))

    for row in rows:
        lcl_id = str(row["lcl_id"]).replace("\t", "").replace("\n", "").strip()
        dt_val = row["reading_datetime"]
        if isinstance(dt_val, datetime):
            ts_str = dt_val.strftime("%Y-%m-%d %H:%M:%S")
        else:
            ts_str = str(dt_val)[:19]
        kwh_val = float(row["kwh"])

        line = f"{lcl_id}\t{ts_str}\t{kwh_val:.4f}\n"
        chunk.append(line)

        if len(chunk) >= CHUNK_SIZE:
            send_chunk(chunk)
            chunk.clear()

    if chunk:
        send_chunk(chunk)
        chunk.clear()


# ---------------------------------------------------------
# Process micro-batches
# ---------------------------------------------------------
def write_batch(batch_df, batch_id):
    num_rows = batch_df.count()
    if num_rows == 0:
        return

    batch_start = time.time()
    batch_df.foreachPartition(write_partition)
    batch_duration = time.time() - batch_start

    rate = num_rows / batch_duration if batch_duration > 0 else 0
    print(
        f"[Batch {batch_id}] Ingested {num_rows:,} rows into ClickHouse in {batch_duration:.2f}s ({rate:,.0f} rows/s)",
        flush=True
    )


# ---------------------------------------------------------
# Start Streaming Query
# ---------------------------------------------------------
checkpoint_dir = "/opt/spark/work/checkpoints/kafka_clickhouse"

query = (
    clean.writeStream
    .foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_dir)
    .trigger(processingTime="5 seconds")
    .start()
)

print(f"Streaming query started. Checkpoint: {checkpoint_dir}. Awaiting data...", flush=True)

query.awaitTermination()