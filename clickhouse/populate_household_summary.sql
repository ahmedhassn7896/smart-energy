-- Populates analytics.household_energy_summary from raw_smart_meter_events.
-- consumer_tier is computed from actual data (terciles of avg daily kWh
-- across all households), not guessed fixed thresholds.
-- Run this ONCE (ReplacingMergeTree — re-running duplicates rows until merged).

INSERT INTO analytics.household_energy_summary
SELECT
    lcl_id,
    first_reading,
    last_reading,
    days_active,
    total_kwh,
    avg_kwh_per_reading,
    avg_daily_kwh,
    min_kwh,
    max_kwh,
    reading_count,
    multiIf(
        avg_daily_kwh < q33, 'Low',
        avg_daily_kwh < q66, 'Medium',
        'High'
    ) AS consumer_tier
FROM
(
    SELECT
        lcl_id,
        min(reading_datetime) AS first_reading,
        max(reading_datetime) AS last_reading,
        uniqExact(toDate(reading_datetime)) AS days_active,
        sum(kwh) AS total_kwh,
        avg(kwh) AS avg_kwh_per_reading,
        sum(kwh) / uniqExact(toDate(reading_datetime)) AS avg_daily_kwh,
        min(kwh) AS min_kwh,
        max(kwh) AS max_kwh,
        count() AS reading_count
    FROM analytics.raw_smart_meter_events
    GROUP BY lcl_id
) AS household_stats
CROSS JOIN
(
    SELECT
        quantile(0.33)(avg_daily_kwh) AS q33,
        quantile(0.66)(avg_daily_kwh) AS q66
    FROM
    (
        SELECT
            lcl_id,
            sum(kwh) / uniqExact(toDate(reading_datetime)) AS avg_daily_kwh
        FROM analytics.raw_smart_meter_events
        GROUP BY lcl_id
    )
) AS quantiles;

-- Sanity checks:
-- SELECT count(*) FROM analytics.household_energy_summary;
-- SELECT consumer_tier, count(*) FROM analytics.household_energy_summary GROUP BY consumer_tier;
