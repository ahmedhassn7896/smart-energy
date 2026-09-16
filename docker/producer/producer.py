import csv
import json
import os
import sys
import time
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "smart_energy")
SOURCE_FILE = os.getenv("SOURCE_FILE", "/opt/data/CC_LCL-FullData.csv")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
BATCH_DELAY_SECONDS = float(os.getenv("BATCH_DELAY_SECONDS", "0.0"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "0"))
START_AFTER = int(os.getenv("START_AFTER", "0"))

print(f"=== Smart Energy Kafka Producer ===", flush=True)
print(f"Source file: {SOURCE_FILE}", flush=True)
print(f"Target topic: {TOPIC}", flush=True)
print(f"Bootstrap servers: {BOOTSTRAP}", flush=True)
print(f"Batch size: {BATCH_SIZE}", flush=True)
print(f"Max rows: {MAX_ROWS} (0 = unlimited)", flush=True)
print(f"Start after: {START_AFTER}", flush=True)

if not os.path.exists(SOURCE_FILE):
    print(f"ERROR: File not found: {SOURCE_FILE}", file=sys.stderr, flush=True)
    sys.exit(1)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    acks=1,
    retries=5,
    linger_ms=10,
    batch_size=131072,
)

def parse_float(value):
    if value is None:
        return None
    value = value.strip()
    if not value or value.lower() in ("null", "none", "nan", "na", ""):
        return None
    try:
        val = float(value)
        if val < 0.0 or val != val:  # filter negative or NaN
            return None
        return val
    except ValueError:
        return None

def parse_timestamp(value):
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if "." in value:
        head, frac = value.split(".", 1)
        frac = "".join(ch for ch in frac if ch.isdigit())[:6]
        value = f"{head}.{frac}" if frac else head
    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def find_column(fieldnames, target):
    """Match a column name ignoring leading/trailing whitespace."""
    for name in fieldnames:
        if name.strip() == target:
            return name
    return None

sent = 0
scanned = 0
skipped = 0
start_time = time.time()
last_report_time = start_time
last_report_sent = 0

with open(SOURCE_FILE, "r", encoding="utf-8", newline="", errors="replace") as f:
    reader = csv.DictReader(f)

    expected_columns = [
        "LCLid",
        "DateTime",
        "KWH/hh (per half hour)"
    ]

    column_map = {}
    for expected in expected_columns:
        actual = find_column(reader.fieldnames or [], expected)
        if actual is None:
            raise RuntimeError(
                f"Missing CSV column matching: {expected!r}. Actual header was: {reader.fieldnames}"
            )
        column_map[expected] = actual

    lclid_col = column_map["LCLid"]
    datetime_col = column_map["DateTime"]
    kwh_col = column_map["KWH/hh (per half hour)"]

    for row in reader:
        scanned += 1

        if scanned <= START_AFTER:
            continue

        lcl_id = (row.get(lclid_col) or "").strip()
        timestamp_raw = (row.get(datetime_col) or "").strip()
        kwh_val = parse_float(row.get(kwh_col))

        if not lcl_id or not timestamp_raw or kwh_val is None:
            skipped += 1
            continue

        ts_clean = parse_timestamp(timestamp_raw)
        if ts_clean is None:
            skipped += 1
            continue

        message = {
            "household_id": lcl_id,
            "timestamp": ts_clean,
            "energy_consumption": round(kwh_val, 4)
        }

        # Key by household_id for consistent Kafka partitioning
        producer.send(TOPIC, key=lcl_id.encode("utf-8"), value=message)
        sent += 1

        if sent % BATCH_SIZE == 0:
            producer.flush()
            now = time.time()
            elapsed = now - start_time
            delta_time = now - last_report_time
            delta_sent = sent - last_report_sent
            current_rate = delta_sent / delta_time if delta_time > 0 else 0
            overall_rate = sent / elapsed if elapsed > 0 else 0

            print(
                f"Progress: sent={sent:,} | skipped={skipped:,} | scanned={scanned:,} | "
                f"rate={current_rate:,.0f} rec/s (avg {overall_rate:,.0f}) | elapsed={elapsed:.1f}s",
                flush=True
            )
            last_report_time = now
            last_report_sent = sent

            if BATCH_DELAY_SECONDS > 0:
                time.sleep(BATCH_DELAY_SECONDS)

        if 0 < MAX_ROWS <= sent:
            break

producer.flush()
producer.close()

total_elapsed = time.time() - start_time
avg_rate = sent / total_elapsed if total_elapsed > 0 else 0
print(
    f"\n=== Producer Finished ===\n"
    f"Total sent: {sent:,}\n"
    f"Total skipped: {skipped:,}\n"
    f"Total scanned: {scanned:,}\n"
    f"Elapsed time: {total_elapsed:.2f}s\n"
    f"Average rate: {avg_rate:,.0f} records/sec\n",
    flush=True
)