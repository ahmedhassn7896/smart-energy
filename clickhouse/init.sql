CREATE DATABASE IF NOT EXISTS analytics;

-- 1. Raw Smart Meter Events Landing Table
CREATE TABLE IF NOT EXISTS analytics.raw_smart_meter_events
(
    lcl_id String,
    reading_datetime DateTime,
    kwh Float64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(reading_datetime)
ORDER BY (lcl_id, reading_datetime);


-- 2. Hourly Energy Aggregates
CREATE TABLE IF NOT EXISTS analytics.hourly_energy_consumption
(
    reading_hour DateTime,
    year UInt16,
    month UInt8,
    day UInt8,
    hour UInt8,
    day_of_week UInt8,
    is_weekend UInt8,
    total_kwh Float64,
    avg_kwh Float64,
    min_kwh Float64,
    max_kwh Float64,
    reading_count UInt64,
    household_count UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(reading_hour)
ORDER BY (reading_hour, hour);


-- 3. Daily Energy Aggregates
CREATE TABLE IF NOT EXISTS analytics.daily_energy_consumption
(
    reading_date Date,
    year UInt16,
    month UInt8,
    day UInt8,
    day_of_week UInt8,
    day_name String,
    is_weekend UInt8,
    total_kwh Float64,
    avg_kwh Float64,
    min_kwh Float64,
    max_kwh Float64,
    reading_count UInt64,
    household_count UInt64,
    peak_hour UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(reading_date)
ORDER BY (reading_date);


-- 4. Monthly Energy Aggregates
CREATE TABLE IF NOT EXISTS analytics.monthly_energy_consumption
(
    year UInt16,
    month UInt8,
    month_name String,
    season String,
    total_kwh Float64,
    avg_daily_kwh Float64,
    avg_kwh Float64,
    min_kwh Float64,
    max_kwh Float64,
    reading_count UInt64,
    household_count UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (year, month);


-- 5. Household Summary
CREATE TABLE IF NOT EXISTS analytics.household_energy_summary
(
    lcl_id String,
    first_reading DateTime,
    last_reading DateTime,
    days_active UInt32,
    total_kwh Float64,
    avg_kwh_per_reading Float64,
    avg_daily_kwh Float64,
    min_kwh Float64,
    max_kwh Float64,
    reading_count UInt64,
    consumer_tier String
)
ENGINE = ReplacingMergeTree
ORDER BY (lcl_id);


-- 6. Energy Anomalies
CREATE TABLE IF NOT EXISTS analytics.energy_anomalies
(
    lcl_id String,
    reading_datetime DateTime,
    kwh Float64,
    expected_kwh Float64,
    baseline_std Float64,
    z_score Float64,
    anomaly_method String,
    severity String,
    detected_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(reading_datetime)
ORDER BY (lcl_id, reading_datetime);


-- 7. Energy Demand Forecasting
CREATE TABLE IF NOT EXISTS analytics.energy_forecast
(
    forecast_date Date,
    actual_kwh Nullable(Float64),
    forecast_kwh Float64,
    lower_bound Float64,
    upper_bound Float64,
    model_name String,
    generated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(generated_at)
ORDER BY (forecast_date, model_name);


-- 8. Power BI KPI & Overview View
CREATE OR REPLACE VIEW analytics.v_powerbi_kpis AS
SELECT
    (SELECT count() FROM analytics.raw_smart_meter_events) AS total_readings,
    (SELECT uniqExact(lcl_id) FROM analytics.raw_smart_meter_events) AS total_households,
    (SELECT sum(kwh) FROM analytics.raw_smart_meter_events) AS total_consumption_kwh,
    (SELECT avg(kwh) FROM analytics.raw_smart_meter_events) AS avg_consumption_kwh,
    (SELECT min(reading_datetime) FROM analytics.raw_smart_meter_events) AS min_reading_date,
    (SELECT max(reading_datetime) FROM analytics.raw_smart_meter_events) AS max_reading_date,
    (SELECT count() FROM analytics.energy_anomalies) AS total_anomalies;
