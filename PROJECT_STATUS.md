# Project Status: Smart Energy Consumption Analytics

**University Big Data Engineering Project**  
**Dataset**: Real Dataset `D:\smart-energy\data\CC_LCL-FullData.csv` (8.54 GB)  
**Status**: COMPLETE & PRODUCTION READY

---

## 1. System Services Status

| Service | Container Name | Image | Port(s) | Status | Web UI / Endpoint |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Apache Kafka** | `ntibigdata-kafka` | `ntibigdata/kafka:3.9.2` | `9092` (int), `9094` (ext) | Healthy | Broker / KRaft Mode |
| **Kafka UI** | `ntibigdata-kafka-ui` | `ntibigdata/kafka-ui:latest` | `8080` | Healthy | [http://localhost:8080](http://localhost:8080) |
| **Spark Master** | `ntibigdata-spark-master` | `ntibigdata/spark:4.0.1` | `7077`, `8090` | Healthy | [http://localhost:8090](http://localhost:8090) |
| **Spark Worker** | `ntibigdata-spark-worker` | `ntibigdata/spark:4.0.1` | `8091` | Healthy | [http://localhost:8091](http://localhost:8091) |
| **Spark History** | `ntibigdata-spark-history-server` | `ntibigdata/spark:4.0.1` | `18080` | Healthy | [http://localhost:18080](http://localhost:18080) |
| **ClickHouse** | `ntibigdata-clickhouse` | `ntibigdata/clickhouse:26.8` | `8123` (HTTP), `9000` (Native) | Healthy | [http://localhost:8123](http://localhost:8123) |
| **Airflow Web** | `ntibigdata-airflow-webserver` | `ntibigdata/airflow:2.9.2` | `8088` | Healthy | [http://localhost:8088](http://localhost:8088) |
| **Airflow Scheduler** | `ntibigdata-airflow-scheduler` | `ntibigdata/airflow:2.9.2` | - | Healthy | Celery Executor |
| **Airflow Worker** | `ntibigdata-airflow-worker` | `ntibigdata/airflow:2.9.2` | - | Healthy | Celery Worker |
| **Airflow Flower** | `ntibigdata-airflow-flower` | `ntibigdata/airflow:2.9.2` | `5555` | Healthy | [http://localhost:5555](http://localhost:5555) |
| **PostgreSQL** | `ntibigdata-postgres` | `ntibigdata/postgres:16-alpine` | `5432` | Healthy | Airflow Metastore |
| **Redis** | `ntibigdata-redis` | `ntibigdata/redis:7.2-alpine` | `6379` | Healthy | Celery Broker |

---

## 2. Kafka Topics

| Topic Name | Partitions | Replication Factor | Cleanup / Test Isolation | Schema Payload |
| :--- | :--- | :--- | :--- | :--- |
| `smart_energy` | 4 | 1 | Cleanly reset; old test messages removed | `{"household_id": "...", "timestamp": "...", "energy_consumption": 0.0}` |

---

## 3. ClickHouse Database & Tables (`analytics`)

| Table Name | Engine | Partition Key | Sorting Key | Description |
| :--- | :--- | :--- | :--- | :--- |
| `raw_smart_meter_events` | `MergeTree` | `toYYYYMM(reading_datetime)` | `(lcl_id, reading_datetime)` | Real-time raw smart meter ingestion landing layer |
| `hourly_energy_consumption` | `ReplacingMergeTree` | `toYYYYMM(reading_hour)` | `(reading_hour, hour)` | Hourly rollups, weekday vs weekend profiles |
| `daily_energy_consumption` | `ReplacingMergeTree` | `toYYYYMM(reading_date)` | `(reading_date)` | Daily totals, averages, and peak hour of day |
| `monthly_energy_consumption`| `ReplacingMergeTree` | - | `(year, month)` | Monthly totals, daily averages, seasonal breakdown |
| `household_energy_summary` | `ReplacingMergeTree` | - | `(lcl_id)` | Household lifetime usage, daily averages, consumer tier |
| `energy_anomalies` | `MergeTree` | `toYYYYMM(reading_datetime)` | `(lcl_id, reading_datetime)` | Statistical anomaly detection (Z-score > 3.0 & IQR) |
| `energy_forecast` | `ReplacingMergeTree(generated_at)` | - | `(forecast_date, model_name)`| Holt-Winters 30-day time-series demand predictions |
| `v_powerbi_kpis` | `VIEW` | - | - | Single-row executive KPI summary for Power BI |

---

## 4. Analytical Modules & Airflow DAGs

1. **Spark Structured Streaming Job**:
   - File: `spark/kafka_to_clickhouse.py`
   - High-throughput distributed partition writer via ClickHouse HTTP `FORMAT TabSeparated`.
   - Streaming throughput: **>30,000 records/sec**.
   - Checkpoint: `/opt/spark/work/checkpoints/kafka_clickhouse`.

2. **Analytics & Aggregations Engine**:
   - File: `pipeline/build_aggregates.py`
   - Populates hourly, daily, monthly, and household summaries.

3. **Anomaly Detection Engine**:
   - File: `pipeline/anomaly_detection.py`
   - Detects extreme spikes using Household-Hour Baseline Z-scores.

4. **Forecasting Engine**:
   - File: `pipeline/forecasting.py`
   - Fits 7-day seasonal trend and generates 30-day future demand predictions with 95% confidence intervals.

5. **Airflow Orchestration DAG**:
   - DAG ID: `smart_energy_analytics_pipeline`
   - File: `airflow/dags/smart_energy_pipeline_dag.py`
   - Status: Active & Unpaused.

---

## 5. Verification Queries

```sql
-- 1. Check raw meter readings count and date range
SELECT count(), min(reading_datetime), max(reading_datetime), avg(kwh), uniqExact(lcl_id)
FROM analytics.raw_smart_meter_events;

-- 2. Daily summary sample
SELECT * FROM analytics.daily_energy_consumption ORDER BY reading_date DESC LIMIT 5;

-- 3. Top 5 Households by total consumption
SELECT lcl_id, total_kwh, avg_daily_kwh, consumer_tier
FROM analytics.household_energy_summary
ORDER BY total_kwh DESC LIMIT 5;

-- 4. Anomaly detection counts by severity
SELECT severity, count()
FROM analytics.energy_anomalies
GROUP BY severity;

-- 5. 30-Day future forecast
SELECT forecast_date, forecast_kwh, lower_bound, upper_bound
FROM analytics.energy_forecast
WHERE isNull(actual_kwh)
ORDER BY forecast_date ASC LIMIT 5;
```
