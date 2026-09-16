# Smart Energy Consumption Analytics - Video & Presentation Demo Guide

This guide details the exact step-by-step recording and presentation flow for demonstrating the completed Big Data pipeline.

---

## 🎬 Suggested Demonstration Sequence

```
1. Architecture & Repository
   ↓
2. Docker Desktop Containers
   ↓
3. Kafka UI & smart_energy Topic
   ↓
4. Real-Time Streaming Producer
   ↓
5. Apache Spark Master & Worker UIs
   ↓
6. Spark Structured Streaming Application
   ↓
7. ClickHouse Real-Time Analytics & Queries
   ↓
8. Apache Airflow Pipeline DAG Run
   ↓
9. Microsoft Power BI Interactive Dashboard
```

---

## 📋 Step-by-Step Presentation Script

### 1. Project Overview & Architecture
- **Screen**: VS Code / `README.md` and terminal.
- **Narrative**:
  - University Big Data project analyzing real-world smart meter consumption across London households using the real 8.54 GB dataset (`CC_LCL-FullData.csv`).
  - Architecture: **Real CSV -> Streaming Kafka Producer -> Apache Kafka (4 partitions) -> Apache Spark 4.0.1 (Structured Streaming) -> ClickHouse 26.8 OLAP -> Statistical Anomaly Detection & Forecasting -> Apache Airflow Orchestration -> Power BI Desktop**.

### 2. Docker Desktop Infrastructure
- **Screen**: Docker Desktop UI or `docker ps` in PowerShell.
- **Key Points**:
  - Show the 10+ running healthy containers: `ntibigdata-kafka`, `ntibigdata-kafka-ui`, `ntibigdata-spark-master`, `ntibigdata-spark-worker`, `ntibigdata-spark-history-server`, `ntibigdata-clickhouse`, `ntibigdata-airflow-webserver`, `ntibigdata-airflow-scheduler`, `ntibigdata-airflow-worker`, `ntibigdata-postgres`, `ntibigdata-redis`.

### 3. Kafka UI & Topic Health
- **URL**: `http://localhost:8080`
- **Action**:
  - Click on **Topics** -> `smart_energy`.
  - Show 4 partitions, partition distribution, message throughput rate, and JSON messages format:
    ```json
    {
      "household_id": "MAC000002",
      "timestamp": "2012-10-12 00:30:00",
      "energy_consumption": 0.2632
    }
    ```

### 4. Spark Master & Worker UIs
- **URL**: `http://localhost:8090` (Spark Master) and `http://localhost:8091` (Worker)
- **Action**:
  - Show the active driver and running application: `SmartEnergyKafkaToClickHouse`.
  - Click into the application to show Structured Streaming micro-batch processing metrics (throughput, processing time per batch, zero failed tasks).

### 5. ClickHouse Real-Time Data & OLAP Queries
- **Action**: Run analytical queries in ClickHouse client or DBeaver / HTTP:
  ```sql
  -- Raw data count & stats
  SELECT count(), uniqExact(lcl_id), min(reading_datetime), max(reading_datetime), avg(kwh)
  FROM analytics.raw_smart_meter_events;

  -- Daily aggregates
  SELECT * FROM analytics.daily_energy_consumption ORDER BY reading_date DESC LIMIT 10;

  -- Top 10 Energy Consumers
  SELECT lcl_id, total_kwh, avg_daily_kwh, consumer_tier
  FROM analytics.household_energy_summary
  ORDER BY total_kwh DESC LIMIT 10;

  -- Detected Energy Anomalies
  SELECT lcl_id, reading_datetime, kwh, expected_kwh, z_score, severity
  FROM analytics.energy_anomalies
  WHERE severity = 'Critical'
  LIMIT 10;

  -- 30-Day Predictive Forecast
  SELECT forecast_date, actual_kwh, forecast_kwh, lower_bound, upper_bound
  FROM analytics.energy_forecast
  ORDER BY forecast_date DESC LIMIT 15;
  ```

### 6. Apache Airflow Orchestration DAG
- **URL**: `http://localhost:8088` (Credentials: `admin` / `admin`)
- **Action**:
  - Open DAG: `smart_energy_analytics_pipeline`.
  - Show the DAG graph view:
    `validate_dataset` -> `check_infrastructure` -> `verify_ingestion` -> `build_daily_aggregates` -> [`run_anomaly_detection`, `run_forecasting`] -> `validate_results`.
  - Show all tasks green (Success status) and inspect task logs.

### 7. Power BI Dashboard Demonstration
- **Screen**: Power BI Desktop (`powerbi/powerbi_setup_guide.md`).
- **Action**:
  - **Executive KPI Cards**: Total Consumption, Average Daily Usage, Household Count, Total Readings, Anomaly Count.
  - **Temporal Charts**: Hourly Weekday vs Weekend Profile, Monthly Seasonal Trends.
  - **Household Analysis**: Top 10 Consumers bar chart, Consumer Tier breakdown.
  - **Anomaly Scatter Plot**: Extreme consumption spikes flagged with Z-scores > 3.0.
  - **Forecasting View**: Actual historical demand vs 30-day predicted consumption with 95% confidence intervals.
  - Interact with date and household slicers to demonstrate real-time dynamic filtering.
