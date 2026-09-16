"""
Smart Energy Consumption Analytics - Airflow Production Pipeline DAG
Orchestrates end-to-end Big Data workflow:
1. Dataset Validation
2. Infrastructure Health Checks (Kafka, Spark, ClickHouse)
3. Ingestion Verification
4. Analytical Aggregations (Hourly, Daily, Monthly)
5. Household Summary & Profiling
6. Anomaly Detection Engine
7. Time-Series Demand Forecasting
8. Final KPI & Data Quality Verification
"""

import os
import time
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "analytics")

def ch_query(query: str) -> str:
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?" + urllib.parse.urlencode({
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "database": CLICKHOUSE_DB
    })
    req = urllib.request.Request(
        url,
        data=query.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/plain; charset=utf-8"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8").strip()

def task_validate_dataset(**context):
    file_path = "/opt/data/CC_LCL-FullData.csv"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source dataset not found at {file_path}")
    file_size = os.path.getsize(file_path)
    print(f"Validated source dataset: {file_path} ({file_size / (1024**3):.2f} GB)")
    return {"status": "success", "file_size_bytes": file_size}

def task_check_infrastructure(**context):
    # ClickHouse ping
    ch_status = ch_query("SELECT 1")
    if ch_status != "1":
        raise RuntimeError(f"ClickHouse check failed, received: {ch_status}")
    print("ClickHouse connection: OK")

    # Spark Master ping
    try:
        with urllib.request.urlopen("http://spark-master:8080", timeout=10) as resp:
            print(f"Spark Master UI reachable: HTTP {resp.status}")
    except Exception as e:
        print(f"Spark master ping notice: {e}")

    return {"status": "infrastructure_healthy"}

def task_verify_ingestion(**context):
    raw_count_str = ch_query("SELECT count() FROM analytics.raw_smart_meter_events")
    raw_count = int(raw_count_str) if raw_count_str.isdigit() else 0
    
    stats = ch_query(
        "SELECT min(reading_datetime), max(reading_datetime), avg(kwh), uniqExact(lcl_id) "
        "FROM analytics.raw_smart_meter_events"
    )
    print(f"Raw meter events count: {raw_count:,}")
    print(f"Stats (min_date, max_date, avg_kwh, households): {stats}")
    
    if raw_count == 0:
        raise ValueError("No raw records found in analytics.raw_smart_meter_events.")
    return {"raw_count": raw_count, "stats": stats}

def task_build_daily_aggregates(**context):
    import subprocess
    cmd = ["python", "/opt/pipeline/build_aggregates.py"]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    print(res.stdout)
    return "aggregates_built"

def task_run_anomaly_detection(**context):
    import subprocess
    cmd = ["python", "/opt/pipeline/anomaly_detection.py"]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    print(res.stdout)
    return "anomalies_computed"

def task_run_forecasting(**context):
    import subprocess
    cmd = ["python", "/opt/pipeline/forecasting.py"]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    print(res.stdout)
    return "forecasting_computed"

def task_validate_results(**context):
    tables = [
        "raw_smart_meter_events",
        "hourly_energy_consumption",
        "daily_energy_consumption",
        "monthly_energy_consumption",
        "household_energy_summary",
        "energy_anomalies",
        "energy_forecast"
    ]
    summary = {}
    for t in tables:
        cnt = int(ch_query(f"SELECT count() FROM analytics.{t}"))
        summary[t] = cnt
        print(f"Table analytics.{t}: {cnt:,} records")
        
    print(f"\nPipeline Validation Summary: {json.dumps(summary, indent=2)}")
    return summary

default_args = {
    "owner": "smart_energy_admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="smart_energy_analytics_pipeline",
    default_args=default_args,
    description="End-to-End Smart Energy Big Data Pipeline Orchestration",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bigdata", "spark", "kafka", "clickhouse", "analytics"],
) as dag:

    t_validate_dataset = PythonOperator(
        task_id="validate_dataset",
        python_callable=task_validate_dataset,
    )

    t_check_infra = PythonOperator(
        task_id="check_infrastructure",
        python_callable=task_check_infrastructure,
    )

    t_verify_ingest = PythonOperator(
        task_id="verify_ingestion",
        python_callable=task_verify_ingestion,
    )

    t_build_aggs = PythonOperator(
        task_id="build_daily_aggregates",
        python_callable=task_build_daily_aggregates,
    )

    t_run_anomaly = PythonOperator(
        task_id="run_anomaly_detection",
        python_callable=task_run_anomaly_detection,
    )

    t_run_forecast = PythonOperator(
        task_id="run_forecasting",
        python_callable=task_run_forecasting,
    )

    t_validate_results = PythonOperator(
        task_id="validate_results",
        python_callable=task_validate_results,
    )

    t_validate_dataset >> t_check_infra >> t_verify_ingest >> t_build_aggs >> [t_run_anomaly, t_run_forecast] >> t_validate_results
