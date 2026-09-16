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
    print("=== Smart Energy Analytics: Building Aggregations ===", flush=True)
    
    # Check raw row count
    raw_count_str = execute_query(
        "SELECT count() FROM analytics.raw_smart_meter_events",
        "Check raw events count"
    )
    raw_count = int(raw_count_str) if raw_count_str.isdigit() else 0
    print(f"Total raw events available: {raw_count:,}", flush=True)
    
    if raw_count == 0:
        print("WARNING: No raw events found in analytics.raw_smart_meter_events yet.", flush=True)
        return
        
    # 1. Populate Hourly Aggregates
    q_hourly = """
    INSERT INTO analytics.hourly_energy_consumption
    SELECT
        toStartOfHour(reading_datetime) AS reading_hour,
        toYear(reading_datetime) AS year,
        toMonth(reading_datetime) AS month,
        toDayOfMonth(reading_datetime) AS day,
        toHour(reading_datetime) AS hour,
        toDayOfWeek(reading_datetime) AS day_of_week,
        if(toDayOfWeek(reading_datetime) IN (6, 7), 1, 0) AS is_weekend,
        round(sum(kwh), 4) AS total_kwh,
        round(avg(kwh), 4) AS avg_kwh,
        round(min(kwh), 4) AS min_kwh,
        round(max(kwh), 4) AS max_kwh,
        count() AS reading_count,
        uniqExact(lcl_id) AS household_count
    FROM analytics.raw_smart_meter_events
    GROUP BY reading_hour, year, month, day, hour, day_of_week, is_weekend
    """
    execute_query(q_hourly, "Populate hourly_energy_consumption")

    # 2. Populate Daily Aggregates with Peak Hour
    q_daily = """
    INSERT INTO analytics.daily_energy_consumption
    WITH hourly_totals AS (
        SELECT
            toDate(reading_datetime) AS d_date,
            toHour(reading_datetime) AS d_hour,
            sum(kwh) AS hour_sum
        FROM analytics.raw_smart_meter_events
        GROUP BY d_date, d_hour
    ),
    daily_peak AS (
        SELECT
            d_date,
            argMax(d_hour, hour_sum) AS peak_h
        FROM hourly_totals
        GROUP BY d_date
    )
    SELECT
        toDate(r.reading_datetime) AS reading_date,
        toYear(r.reading_datetime) AS year,
        toMonth(r.reading_datetime) AS month,
        toDayOfMonth(r.reading_datetime) AS day,
        toDayOfWeek(r.reading_datetime) AS day_of_week,
        case toDayOfWeek(r.reading_datetime)
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
            when 7 then 'Sunday'
            else 'Unknown'
        end AS day_name,
        if(toDayOfWeek(r.reading_datetime) IN (6, 7), 1, 0) AS is_weekend,
        round(sum(r.kwh), 4) AS total_kwh,
        round(avg(r.kwh), 4) AS avg_kwh,
        round(min(r.kwh), 4) AS min_kwh,
        round(max(r.kwh), 4) AS max_kwh,
        count() AS reading_count,
        uniqExact(r.lcl_id) AS household_count,
        any(dp.peak_h) AS peak_hour
    FROM analytics.raw_smart_meter_events r
    LEFT JOIN daily_peak dp ON toDate(r.reading_datetime) = dp.d_date
    GROUP BY reading_date, year, month, day, day_of_week, day_name, is_weekend
    """
    execute_query(q_daily, "Populate daily_energy_consumption")

    # 3. Populate Monthly Aggregates
    q_monthly = """
    INSERT INTO analytics.monthly_energy_consumption
    SELECT
        toYear(reading_datetime) AS year,
        toMonth(reading_datetime) AS month,
        case toMonth(reading_datetime)
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
            else 'Unknown'
        end AS month_name,
        case
            when toMonth(reading_datetime) in (12, 1, 2) then 'Winter'
            when toMonth(reading_datetime) in (3, 4, 5) then 'Spring'
            when toMonth(reading_datetime) in (6, 7, 8) then 'Summer'
            else 'Autumn'
        end AS season,
        round(sum(kwh), 4) AS total_kwh,
        round(sum(kwh) / uniqExact(toDate(reading_datetime)), 4) AS avg_daily_kwh,
        round(avg(kwh), 4) AS avg_kwh,
        round(min(kwh), 4) AS min_kwh,
        round(max(kwh), 4) AS max_kwh,
        count() AS reading_count,
        uniqExact(lcl_id) AS household_count
    FROM analytics.raw_smart_meter_events
    GROUP BY year, month, month_name, season
    """
    execute_query(q_monthly, "Populate monthly_energy_consumption")

    # 4. Populate Household Summary & Classification
    q_household = """
    INSERT INTO analytics.household_energy_summary
    WITH hh_stats AS (
        SELECT
            lcl_id,
            min(reading_datetime) AS first_r,
            max(reading_datetime) AS last_r,
            uniqExact(toDate(reading_datetime)) AS days_act,
            sum(kwh) AS tot_kwh,
            avg(kwh) AS avg_per_r,
            min(kwh) AS min_k,
            max(kwh) AS max_k,
            count() AS r_count
        FROM analytics.raw_smart_meter_events
        GROUP BY lcl_id
    )
    SELECT
        lcl_id,
        first_r AS first_reading,
        last_r AS last_reading,
        days_act AS days_active,
        round(tot_kwh, 4) AS total_kwh,
        round(avg_per_r, 4) AS avg_kwh_per_reading,
        round(tot_kwh / greatest(days_act, 1), 4) AS avg_daily_kwh,
        round(min_k, 4) AS min_kwh,
        round(max_k, 4) AS max_kwh,
        r_count AS reading_count,
        case
            when (tot_kwh / greatest(days_act, 1)) > 15.0 then 'High Consumer'
            when (tot_kwh / greatest(days_act, 1)) >= 5.0 then 'Medium Consumer'
            else 'Low Consumer'
        end AS consumer_tier
    FROM hh_stats
    """
    execute_query(q_household, "Populate household_energy_summary")
    
    print("\n=== Aggregation Summary ===", flush=True)
    for tbl in ["hourly_energy_consumption", "daily_energy_consumption", "monthly_energy_consumption", "household_energy_summary"]:
        cnt = execute_query(f"SELECT count() FROM analytics.{tbl}")
        print(f"Table analytics.{tbl}: {int(cnt):,} rows", flush=True)

if __name__ == "__main__":
    main()
