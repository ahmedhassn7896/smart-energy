-- ============================================================
-- Populates hourly / daily / monthly aggregate tables from the
-- 10.7M rows already loaded into analytics.raw_smart_meter_events.
-- Run each block once (ReplacingMergeTree, so re-running just adds
-- duplicate versions — safe, but no need to run twice).
-- ============================================================

-- 1) HOURLY
INSERT INTO analytics.hourly_energy_consumption
SELECT
    reading_hour,
    toYear(reading_hour)      AS year,
    toMonth(reading_hour)     AS month,
    toDayOfMonth(reading_hour) AS day,
    toHour(reading_hour)      AS hour,
    toDayOfWeek(reading_hour) AS day_of_week,
    if(toDayOfWeek(reading_hour) IN (6, 7), 1, 0) AS is_weekend,
    total_kwh,
    avg_kwh,
    min_kwh,
    max_kwh,
    reading_count,
    household_count
FROM
(
    SELECT
        toStartOfHour(reading_datetime) AS reading_hour,
        sum(kwh)        AS total_kwh,
        avg(kwh)        AS avg_kwh,
        min(kwh)        AS min_kwh,
        max(kwh)        AS max_kwh,
        count()         AS reading_count,
        uniqExact(lcl_id) AS household_count
    FROM analytics.raw_smart_meter_events
    GROUP BY reading_hour
);

-- 2) DAILY (includes peak_hour = the hour with highest total kWh that day)
INSERT INTO analytics.daily_energy_consumption
SELECT
    daily_stats.reading_date,
    toYear(daily_stats.reading_date)  AS year,
    toMonth(daily_stats.reading_date) AS month,
    toDayOfMonth(daily_stats.reading_date) AS day,
    toDayOfWeek(daily_stats.reading_date)  AS day_of_week,
    ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][toDayOfWeek(daily_stats.reading_date)] AS day_name,
    if(toDayOfWeek(daily_stats.reading_date) IN (6, 7), 1, 0) AS is_weekend,
    total_kwh,
    avg_kwh,
    min_kwh,
    max_kwh,
    reading_count,
    household_count,
    peak_stats.peak_hour
FROM
(
    SELECT
        toDate(reading_datetime) AS reading_date,
        sum(kwh)        AS total_kwh,
        avg(kwh)        AS avg_kwh,
        min(kwh)        AS min_kwh,
        max(kwh)        AS max_kwh,
        count()         AS reading_count,
        uniqExact(lcl_id) AS household_count
    FROM analytics.raw_smart_meter_events
    GROUP BY reading_date
) AS daily_stats
INNER JOIN
(
    SELECT reading_date, argMax(hour, hour_total) AS peak_hour
    FROM
    (
        SELECT
            toDate(reading_datetime) AS reading_date,
            toHour(reading_datetime) AS hour,
            sum(kwh) AS hour_total
        FROM analytics.raw_smart_meter_events
        GROUP BY reading_date, hour
    )
    GROUP BY reading_date
) AS peak_stats
ON daily_stats.reading_date = peak_stats.reading_date;

-- 3) MONTHLY
INSERT INTO analytics.monthly_energy_consumption
SELECT
    year,
    month,
    ['January','February','March','April','May','June','July','August','September','October','November','December'][month] AS month_name,
    multiIf(month IN (12, 1, 2), 'Winter', month IN (3, 4, 5), 'Spring', month IN (6, 7, 8), 'Summer', 'Autumn') AS season,
    total_kwh,
    total_kwh / days_count AS avg_daily_kwh,
    avg_kwh,
    min_kwh,
    max_kwh,
    reading_count,
    household_count
FROM
(
    SELECT
        toYear(reading_datetime)  AS year,
        toMonth(reading_datetime) AS month,
        sum(kwh)        AS total_kwh,
        avg(kwh)        AS avg_kwh,
        min(kwh)        AS min_kwh,
        max(kwh)        AS max_kwh,
        count()         AS reading_count,
        uniqExact(lcl_id) AS household_count,
        uniqExact(toDate(reading_datetime)) AS days_count
    FROM analytics.raw_smart_meter_events
    GROUP BY year, month
);

-- ============================================================
-- Quick sanity checks after running the above
-- ============================================================
-- SELECT count(*) FROM analytics.hourly_energy_consumption;
-- SELECT count(*) FROM analytics.daily_energy_consumption;
-- SELECT count(*) FROM analytics.monthly_energy_consumption;
-- SELECT * FROM analytics.monthly_energy_consumption ORDER BY year, month;
