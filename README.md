# Smart Energy Consumption Analytics

> **A production-grade Big Data Engineering & Analytics platform processing the real London Smart Meter Dataset (~8.54 GB, ~167 million half-hourly readings) through a fully containerized, end-to-end streaming, batch, and AI-powered pipeline.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9.2-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-4.0.1-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-26.8-FFCC01?logo=clickhouse)](https://clickhouse.com/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.2-017CEE?logo=apacheairflow)](https://airflow.apache.org/)

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Business Problem](#-business-problem)
- [Dataset Description](#-dataset-description)
- [Architecture](#️-system-architecture)
- [Data Flow](#-data-flow)
- [Technology Stack](#-technology-stack)
- [Directory Structure](#-directory-structure)
- [Quick Start](#-quick-start)
- [Service URLs & Ports](#-service-urls--ports)
- [Kafka Topic](#-kafka-topic)
- [Running the Producer](#-running-the-kafka-producer)
- [Running Spark Streaming](#-running-spark-structured-streaming)
- [ClickHouse Schema & Queries](#-clickhouse-analytical-schema)
- [Airflow DAG](#-airflow-orchestration)
- [Power BI Integration](#-power-bi-integration)
- [Dataset Placement (Local Setup)](#-dataset-placement-local-setup)

---

## 🎯 Project Overview

This project implements an **enterprise-grade streaming and batch analytics platform** for smart energy consumption data. It ingests 8.54 GB of real half-hourly smart meter readings from London households, streams them through Apache Kafka, processes them with Apache Spark Structured Streaming, stores them in ClickHouse OLAP, and delivers actionable insights through anomaly detection, demand forecasting, and Power BI dashboards.

The entire infrastructure runs inside Docker containers, orchestrated by Apache Airflow, making it reproducible and portable.

---

## 💼 Business Problem

Energy providers and grid operators need real-time visibility into household consumption patterns to:

- **Detect consumption anomalies** (faulty meters, unusual spikes, energy theft)
- **Forecast demand** to balance grid load and optimize procurement
- **Profile households** by consumption tier for targeted efficiency programs
- **Identify peak hours** to manage demand-response pricing
- **Track seasonal trends** (Winter peaks, Summer air-conditioning loads)

This platform addresses all of these needs using real data from the Low Carbon London (LCL) smart metering trial.

---

## 📊 Dataset Description

| Property | Detail |
|---|---|
| **Name** | Low Carbon London (LCL) Smart Meter Dataset |
| **File** | `CC_LCL-FullData.csv` |
| **Size** | ~8.54 GB |
| **Rows** | ~167 million half-hourly energy readings |
| **Period** | November 2011 – February 2014 |
| **Households** | ~5,500 London households |
| **Granularity** | 30-minute intervals |
| **Source** | UK Power Networks / Kaggle (Low Carbon London) |

### CSV Schema

| Column | Description |
|---|---|
| `LCLid` | Household identifier (e.g., `MAC000002`) |
| `DateTime` | Reading timestamp in ISO format (e.g., `2012-10-12 00:30:00`) |
| `KWH/hh (per half hour)` | Energy consumed in that 30-minute interval (kWh) |

> ⚠️ **The 8.54 GB CSV file is NOT included in this repository** due to its size. See [Dataset Placement](#-dataset-placement-local-setup) below.

---

## 🏗️ System Architecture

```
                         [ CC_LCL-FullData.csv ]
                              (~8.54 GB CSV)
                                    │
                                    ▼
                   [ Kafka Streaming Producer (Python) ]
                     Reads CSV in batches, validates &
                     normalises each row, keys by LCLid
                                    │
                                    ▼
                    [ Apache Kafka 3.9.2 (KRaft Mode) ]
                      Topic: smart_energy | 4 Partitions
                      Replication Factor: 1
                                    │
                                    ▼
                 [ Apache Spark 4.0.1 Structured Streaming ]
                   Micro-batch JSON parsing, validation,
                   distributed ClickHouse HTTP sink
                   (foreachPartition → chunked TabSeparated)
                                    │
                                    ▼
                     [ ClickHouse 26.8 OLAP Database ]
                       Database: analytics
                       Engine: MergeTree / ReplacingMergeTree
                                    │
               ┌─────────────────────┴──────────────────────┐
               ▼                                            ▼
    [ Batch Aggregations ]                   [ Machine Learning & AI ]
    - Hourly rollups                         - Z-Score Anomaly Detection
    - Daily totals + peak hour               - Holt-Winters 30-day Forecast
    - Monthly seasonal trends
    - Household consumer tiering
               │                                            │
               └─────────────────────┬──────────────────────┘
                                    │
                                    ▼
                  [ Apache Airflow 2.9.2 Orchestration ]
                    DAG: smart_energy_analytics_pipeline
                    Schedule: @daily | CeleryExecutor
                                    │
                                    ▼
                       [ Microsoft Power BI Desktop ]
                         DirectQuery via ClickHouse JDBC
                         Executive Dashboards & KPI Cards
```

---

## 🔄 Data Flow

```
CSV → Kafka Producer → Kafka (smart_energy topic) → Spark Structured Streaming
    → ClickHouse (analytics.raw_smart_meter_events)
    → Aggregations (hourly / daily / monthly / household)
    → Anomaly Detection (Z-Score)
    → Demand Forecasting (Holt-Winters)
    → Airflow Orchestration
    → Power BI Dashboards
```

**JSON message format on the Kafka topic:**
```json
{
  "household_id": "MAC000002",
  "timestamp": "2012-10-12 00:30:00",
  "energy_consumption": 0.2632
}
```

---

## 🚀 Technology Stack

| Component | Technology | Version |
|---|---|---|
| **Container Runtime** | Docker Desktop + Compose | Latest |
| **Message Broker** | Apache Kafka (KRaft, no ZooKeeper) | 3.9.2 |
| **Kafka UI** | Provectus Kafka UI | Latest |
| **Stream Processor** | Apache Spark Structured Streaming (PySpark) | 4.0.1 + Java 21 |
| **OLAP Warehouse** | ClickHouse | 26.8 |
| **Orchestration** | Apache Airflow (CeleryExecutor) | 2.9.2 |
| **Task Queue** | Celery + Redis | 7.2 |
| **Metadata Store** | PostgreSQL | 16 |
| **Analytics Language** | Python | 3.12 |
| **BI Tool** | Microsoft Power BI Desktop | Latest |

---

## 📁 Directory Structure

```
smart-energy/
│
├── docker-compose.yml          # Main Docker Compose – all 12+ services
│
├── .env.example                # Environment variable template (copy to .env)
├── .gitignore                  # Excludes data, secrets, runtime artifacts
│
├── README.md                   # This file
├── PROJECT_STATUS.md           # Detailed service & data status
├── DEMO_GUIDE.md               # Step-by-step demonstration guide
│
├── data/                       # ← Place CC_LCL-FullData.csv HERE (not in Git)
│   └── .gitkeep
│
├── docker/                     # Dockerfiles for custom images
│   ├── airflow/                # Airflow image (extends apache/airflow:2.9.2)
│   ├── clickhouse/             # ClickHouse image + MergeTree config
│   │   └── config.d/
│   │       └── merge_tree.xml  # Performance tuning
│   ├── kafka/                  # Kafka KRaft image + topic init script
│   │   └── init-topics.sh
│   ├── kafka-ui/               # Kafka UI image
│   ├── postgres/               # PostgreSQL image (Airflow metadata)
│   ├── producer/               # Python Kafka producer Docker image
│   │   ├── Dockerfile
│   │   └── producer.py         # Main producer script
│   ├── redis/                  # Redis image (Celery broker)
│   └── spark/                  # Spark 4.0.1 + Java 21 image
│       ├── Dockerfile
│       └── spark-defaults.conf # Event logging configuration
│
├── spark/                      # PySpark applications
│   ├── kafka_to_clickhouse.py  # Main Spark Structured Streaming job
│   └── kafka_stream_test.py    # Quick connectivity test
│
├── clickhouse/                 # ClickHouse database initialisation
│   └── init.sql                # CREATE DATABASE + 7 tables + 1 KPI view
│
├── pipeline/                   # Analytical batch modules
│   ├── build_aggregates.py     # Hourly/Daily/Monthly/Household aggregations
│   ├── anomaly_detection.py    # Z-Score based anomaly flagging
│   └── forecasting.py          # Holt-Winters 30-day demand forecasting
│
├── airflow/                    # Airflow DAG definitions
│   ├── dags/
│   │   └── smart_energy_pipeline_dag.py  # Production orchestration DAG
│   ├── logs/                   # Runtime logs (not committed)
│   └── plugins/
│
├── powerbi/                    # Power BI preparation files
│   ├── queries.sql             # ClickHouse SQL queries for Power BI data
│   ├── dax_measures.txt        # DAX measure definitions
│   └── powerbi_setup_guide.md  # Step-by-step Power BI connection guide
│
├── setup.ps1                   # Full infrastructure setup PowerShell script
├── open_project_urls.ps1       # Opens all web UIs in the browser
└── test_all_endpoints.ps1      # Health-check all service endpoints
```

---

## ⚡ Quick Start

### Prerequisites

- Docker Desktop (with WSL2 backend on Windows)
- At least 16 GB RAM allocated to Docker
- The dataset file: `CC_LCL-FullData.csv` placed in `./data/`

### 1. Clone the Repository

```bash
git clone https://github.com/MohammedAhmed-01/smart-energy.git
cd smart-energy
```

### 2. Configure Environment Variables

```powershell
# Copy the template and edit it
Copy-Item .env.example .env
notepad .env
```

### 3. Place the Dataset

```
D:\smart-energy\data\CC_LCL-FullData.csv
```

See [Dataset Placement](#-dataset-placement-local-setup) for instructions.

### 4. Start All Infrastructure Services

```powershell
docker compose up -d
```

### 5. Open All Web UIs Automatically

```powershell
.\open_project_urls.ps1
```

---

## 🌐 Service URLs & Ports

| Service | Port | Local URL | Default Credentials |
|:---|:---|:---|:---|
| **Kafka UI** | `8080` | http://localhost:8080 | — |
| **Spark Master UI** | `8090` | http://localhost:8090 | — |
| **Spark Worker UI** | `8091` | http://localhost:8091 | — |
| **Spark History Server** | `18080` | http://localhost:18080 | — |
| **Airflow Web UI** | `8088` | http://localhost:8088 | `admin` / *(set in .env)* |
| **Flower (Celery)** | `5555` | http://localhost:5555 | — |
| **ClickHouse HTTP** | `8123` | http://localhost:8123 | `default` / *(set in .env)* |
| **ClickHouse Native** | `9000` | `localhost:9000` | `default` / *(set in .env)* |
| **Kafka External** | `9094` | `localhost:9094` | — |

> All credentials are set via environment variables in your `.env` file. See `.env.example` for the required variables.

---

## 🗂 Kafka Topic

| Property | Value |
|---|---|
| **Topic Name** | `smart_energy` |
| **Partitions** | 4 |
| **Replication Factor** | 1 |
| **Key** | `household_id` (for consistent partition routing) |

### Create / Verify the Topic

```powershell
# Create (if not already done by kafka-topic-init service)
docker exec ntibigdata-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --create --if-not-exists `
  --topic smart_energy --partitions 4 --replication-factor 1

# Verify
docker exec ntibigdata-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 `
  --describe --topic smart_energy
```

---

## 📨 Running the Kafka Producer

The producer reads the CSV file in streaming batches and publishes messages to the `smart_energy` Kafka topic.

### Full Dataset (MAX_ROWS=0 = unlimited)

```powershell
docker compose --profile manual run --rm -e MAX_ROWS=0 -e BATCH_SIZE=25000 producer
```

### Controlled Test (e.g. 10,000 rows)

```powershell
docker compose --profile manual run --rm -e MAX_ROWS=10000 producer
```

### Resume from a Checkpoint Row

```powershell
# Skip the first 5,000,000 rows and continue from there
docker compose --profile manual run --rm -e START_AFTER=5000000 -e MAX_ROWS=0 producer
```

### Producer Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MAX_ROWS` | `0` | Maximum rows to send. **0 = unlimited (full dataset)** |
| `BATCH_SIZE` | `10000` | Rows per flush to Kafka |
| `BATCH_DELAY_SECONDS` | `0.0` | Optional delay between batches |
| `START_AFTER` | `0` | Skip the first N rows (resume support) |
| `KAFKA_TOPIC` | `smart_energy` | Kafka topic name |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |

---

## ⚙️ Running Spark Structured Streaming

The Spark job reads from the `smart_energy` Kafka topic, parses JSON, validates data, and writes to ClickHouse in distributed chunks via the HTTP API.

### Start the Streaming Job

```powershell
docker exec ntibigdata-spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 `
  /opt/spark-apps/kafka_to_clickhouse.py
```

### Key Design Points

- **No driver memory collection**: `foreachPartition` writes directly from each executor to ClickHouse — no `collect()` or `list()` of entire partitions.
- **Chunked writes**: Each partition is written in 10,000-row chunks to ClickHouse via `FORMAT TabSeparated` HTTP inserts with retry logic (3 attempts, exponential backoff).
- **Checkpointing**: Checkpoint stored at `/opt/spark/work/checkpoints/kafka_clickhouse` — safe to restart.
- **Restart safety**: `startingOffsets: earliest` with checkpointing ensures no data loss on restart.
- **Trigger**: Every 5 seconds micro-batch.

---

## 🗄️ ClickHouse Analytical Schema

### Access ClickHouse

```powershell
# HTTP query (uses CLICKHOUSE_PASSWORD from your .env)
curl -u default:YOUR_PASSWORD "http://localhost:8123/?query=SELECT+count()+FROM+analytics.raw_smart_meter_events"

# Interactive client
docker exec -it ntibigdata-clickhouse clickhouse-client --user default --password YOUR_PASSWORD
```

### Database: `analytics`

| Table | Engine | Description |
|---|---|---|
| `raw_smart_meter_events` | `MergeTree` | Raw half-hourly readings: `lcl_id`, `reading_datetime`, `kwh`, `ingested_at` |
| `hourly_energy_consumption` | `ReplacingMergeTree` | Hourly aggregates with weekday/weekend flag |
| `daily_energy_consumption` | `ReplacingMergeTree` | Daily totals, averages, and peak hour |
| `monthly_energy_consumption` | `ReplacingMergeTree` | Monthly trends and seasonal breakdown |
| `household_energy_summary` | `ReplacingMergeTree` | Per-household lifetime stats and consumer tier |
| `energy_anomalies` | `MergeTree` | Z-score anomalies flagged as Warning / High / Critical |
| `energy_forecast` | `ReplacingMergeTree` | Holt-Winters 30-day demand forecast with confidence intervals |
| `v_powerbi_kpis` | `VIEW` | Single-row executive KPI summary for Power BI |

### Sample Verification Queries

```sql
-- Total readings and date range
SELECT count(), min(reading_datetime), max(reading_datetime), avg(kwh), uniqExact(lcl_id)
FROM analytics.raw_smart_meter_events;

-- Daily trend (last 10 days)
SELECT * FROM analytics.daily_energy_consumption ORDER BY reading_date DESC LIMIT 10;

-- Top 5 energy consumers
SELECT lcl_id, total_kwh, avg_daily_kwh, consumer_tier
FROM analytics.household_energy_summary
ORDER BY total_kwh DESC LIMIT 5;

-- Anomaly severity breakdown
SELECT severity, count() FROM analytics.energy_anomalies GROUP BY severity;

-- 30-day forecast
SELECT forecast_date, forecast_kwh, lower_bound, upper_bound
FROM analytics.energy_forecast WHERE isNull(actual_kwh) ORDER BY forecast_date;
```

### Run Analytical Aggregations Manually

```powershell
# Build all aggregation tables
docker exec ntibigdata-airflow-scheduler python /opt/pipeline/build_aggregates.py

# Run anomaly detection
docker exec ntibigdata-airflow-scheduler python /opt/pipeline/anomaly_detection.py

# Generate 30-day demand forecast
docker exec ntibigdata-airflow-scheduler python /opt/pipeline/forecasting.py
```

---

## 🔧 Airflow Orchestration

### Access Airflow

- **URL**: http://localhost:8088
- **Credentials**: Set `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` in your `.env` file.

### DAG: `smart_energy_analytics_pipeline`

Runs `@daily` and orchestrates the full pipeline:

```
validate_dataset
      │
check_infrastructure
      │
verify_ingestion
      │
build_daily_aggregates
      │
    ┌─┴─┐
run_anomaly_detection   run_forecasting
    └─┬─┘
      │
validate_results
```

### Trigger the DAG Manually

```powershell
docker exec ntibigdata-airflow-scheduler airflow dags trigger smart_energy_analytics_pipeline
```

### DAG Tasks

| Task ID | Description |
|---|---|
| `validate_dataset` | Checks that `CC_LCL-FullData.csv` is present and readable |
| `check_infrastructure` | Pings ClickHouse and Spark Master for health |
| `verify_ingestion` | Confirms raw meter event count > 0 in ClickHouse |
| `build_daily_aggregates` | Runs `pipeline/build_aggregates.py` |
| `run_anomaly_detection` | Runs `pipeline/anomaly_detection.py` |
| `run_forecasting` | Runs `pipeline/forecasting.py` |
| `validate_results` | Reports row counts for all 7 analytics tables |

---

## 📈 Power BI Integration

> **Note**: The Power BI files in `powerbi/` are **preparation files** — SQL queries, DAX measures, and setup instructions. Actual Power BI Desktop connection requires the ClickHouse JDBC/ODBC driver to be installed locally.

### Connection Details (use values from your `.env`)

| Setting | Value |
|---|---|
| **Host** | `localhost` (or `CLICKHOUSE_HOST`) |
| **HTTP Port** | `8123` (or `CLICKHOUSE_HTTP_PORT`) |
| **Database** | `analytics` (or `CLICKHOUSE_DB`) |
| **Username** | `default` (or `CLICKHOUSE_USER`) |
| **Password** | *(set `CLICKHOUSE_PASSWORD` in your `.env`)* |

### Power BI Files

| File | Purpose |
|---|---|
| [`powerbi/powerbi_setup_guide.md`](powerbi/powerbi_setup_guide.md) | Step-by-step connection guide |
| [`powerbi/queries.sql`](powerbi/queries.sql) | Optimised ClickHouse SQL for each dashboard page |
| [`powerbi/dax_measures.txt`](powerbi/dax_measures.txt) | Pre-built DAX measure definitions |

---

## 📂 Dataset Placement (Local Setup)

The 8.54 GB dataset is **not included in this repository**. You must obtain and place it manually.

### Step 1: Download the Dataset

The dataset is the **Low Carbon London Smart Meter Dataset** available from:
- [Kaggle - Low Carbon London](https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london)
- [UK Power Networks Open Data Portal](https://ukpowernetworks.opendatasoft.com/)

Download the file: `CC_LCL-FullData.csv`

### Step 2: Place the File

```
D:\smart-energy\data\CC_LCL-FullData.csv
```

The `./data/` directory is mapped into the producer container at `/opt/data/`. Do **not** place any other large files in this directory.

### Step 3: Verify

```powershell
# Should show ~8.54 GB
Get-Item D:\smart-energy\data\CC_LCL-FullData.csv | Select-Object Name, @{N='SizeGB';E={[math]::Round($_.Length/1GB,2)}}
```

---

## 🔐 Security Notes

- **Never commit** your `.env` file or any file containing real passwords, API keys, or secrets.
- All passwords in `docker-compose.yml` use environment variable substitution (`${VAR:-default}`).
- For production deployment, replace all `change_me` placeholders in `.env` with strong, randomly generated values.
- The Airflow Fernet key should be generated with: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`

---

## 📄 License

This project is for academic / educational purposes using publicly available data from the Low Carbon London smart metering trial conducted by UK Power Networks.
