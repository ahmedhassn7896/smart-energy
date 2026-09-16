-- =========================================================
-- Smart Energy Consumption Analytics - Power BI Data Layer
-- Optimized ClickHouse SQL Queries for Power BI Desktop
-- =========================================================

-- 1. Executive Summary & Overall KPIs
SELECT
    count() AS total_readings,
    uniqExact(lcl_id) AS total_households,
    round(sum(kwh), 2) AS total_consumption_kwh,
    round(avg(kwh), 4) AS avg_kwh_per_reading,
    round(min(kwh), 4) AS min_kwh,
    round(max(kwh), 4) AS max_kwh,
    min(reading_datetime) AS dataset_start,
    max(reading_datetime) AS dataset_end
FROM analytics.raw_smart_meter_events;


-- 2. Daily Consumption Trend (For Time-Series Line Charts & Slicers)
SELECT
    reading_date,
    year,
    month,
    day,
    day_name,
    is_weekend,
    total_kwh,
    avg_kwh,
    reading_count,
    household_count,
    peak_hour
FROM analytics.daily_energy_consumption
ORDER BY reading_date ASC;


-- 3. Hourly Profile & Peak Analysis (For Heatmaps & Hourly Bar Charts)
SELECT
    hour,
    is_weekend,
    round(avg(avg_kwh), 4) AS avg_hourly_kwh,
    round(sum(total_kwh), 2) AS total_kwh,
    sum(reading_count) AS total_readings
FROM analytics.hourly_energy_consumption
GROUP BY hour, is_weekend
ORDER BY hour, is_weekend;


-- 4. Monthly & Seasonal Trends (For Seasonality Charts)
SELECT
    year,
    month,
    month_name,
    season,
    total_kwh,
    avg_daily_kwh,
    avg_kwh,
    household_count
FROM analytics.monthly_energy_consumption
ORDER BY year, month;


-- 5. Household Comparison & Top 10 Consumers (For Household Bar Charts & Tier Segmentation)
SELECT
    lcl_id,
    first_reading,
    last_reading,
    days_active,
    total_kwh,
    avg_daily_kwh,
    reading_count,
    consumer_tier
FROM analytics.household_energy_summary
ORDER BY total_kwh DESC;


-- 6. Energy Anomalies (For Anomaly Scatter Plot & Severity Breakdown)
SELECT
    lcl_id,
    reading_datetime,
    kwh,
    expected_kwh,
    baseline_std,
    z_score,
    anomaly_method,
    severity,
    detected_at
FROM analytics.energy_anomalies
ORDER BY z_score DESC;


-- 7. Actual vs Demand Forecast (For 30-Day Predictive Line Chart)
SELECT
    forecast_date,
    actual_kwh,
    forecast_kwh,
    lower_bound,
    upper_bound,
    model_name
FROM analytics.energy_forecast
ORDER BY forecast_date ASC;
