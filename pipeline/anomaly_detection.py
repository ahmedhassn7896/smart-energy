import os
import sys
import time
import urllib.request
import urllib.parse

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "analytics")

def execute_query(query, description=""):
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?" + urllib.parse.urlencode({
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "database": CLICKHOUSE_DB
    })
    
    if description:
        print(f"[{description}] Executing...", flush=True)
        
    req = urllib.request.Request(
        url,
        data=query.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/plain; charset=utf-8"}
    )
    
    start_time = time.time()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            output = resp.read().decode("utf-8").strip()
            elapsed = time.time() - start_time
            if description:
                print(f"[{description}] Done in {elapsed:.2f}s", flush=True)
            return output
    except Exception as ex:
        print(f"ERROR executing [{description}]: {ex}", file=sys.stderr, flush=True)
        raise

def main():
    print("=== Smart Energy Analytics: Anomaly Detection Engine ===", flush=True)
    
    # Truncate anomalies table before repopulating
    execute_query("TRUNCATE TABLE IF EXISTS analytics.energy_anomalies", "Clean anomalies table")
    
    # Anomaly Detection using Household-Hour Baseline Z-Score
    # We calculate the mean and std dev of consumption for each household during each hour of the day.
    # Any reading where Z-score > 3.0 (and consumption > 1.0 kWh) is flagged as an anomaly.
    q_detect = """
    INSERT INTO analytics.energy_anomalies
    WITH baseline AS (
        SELECT
            lcl_id,
            toHour(reading_datetime) AS h_of_day,
            avg(kwh) AS avg_kwh,
            stddevPop(kwh) AS std_kwh
        FROM analytics.raw_smart_meter_events
        GROUP BY lcl_id, h_of_day
        HAVING std_kwh > 0.01
    )
    SELECT
        r.lcl_id,
        r.reading_datetime,
        r.kwh,
        round(b.avg_kwh, 4) AS expected_kwh,
        round(b.std_kwh, 4) AS baseline_std,
        round((r.kwh - b.avg_kwh) / b.std_kwh, 2) AS z_score,
        'Z-Score (Household-Hour Baseline)' AS anomaly_method,
        case
            when (r.kwh - b.avg_kwh) / b.std_kwh >= 5.0 then 'Critical'
            when (r.kwh - b.avg_kwh) / b.std_kwh >= 3.5 then 'High'
            else 'Warning'
        end AS severity,
        now() AS detected_at
    FROM analytics.raw_smart_meter_events r
    JOIN baseline b ON r.lcl_id = b.lcl_id AND toHour(r.reading_datetime) = b.h_of_day
    WHERE (r.kwh - b.avg_kwh) / b.std_kwh >= 3.0
      AND r.kwh > (b.avg_kwh + 0.5)
    ORDER BY z_score DESC
    """
    
    execute_query(q_detect, "Detect and store energy consumption anomalies")
    
    cnt = execute_query("SELECT count() FROM analytics.energy_anomalies")
    crit = execute_query("SELECT count() FROM analytics.energy_anomalies WHERE severity = 'Critical'")
    high = execute_query("SELECT count() FROM analytics.energy_anomalies WHERE severity = 'High'")
    
    print(f"\n=== Anomaly Detection Complete ===", flush=True)
    print(f"Total Anomalies Detected: {int(cnt):,}", flush=True)
    print(f"Critical Severity: {int(crit):,}", flush=True)
    print(f"High Severity: {int(high):,}", flush=True)

if __name__ == "__main__":
    main()
