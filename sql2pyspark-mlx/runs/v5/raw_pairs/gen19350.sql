WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        firmware,
        CASE WHEN device_code IN ('DV-03', 'DV-01') THEN 'Group A' ELSE 'Other' END AS device_code_group
    FROM device_readings
    WHERE humidity < 56
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        firmware,
        device_code_group
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM latest
    EXCEPT ALL
    SELECT id
    FROM device_readings
    WHERE temp_c > 36
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY humidity NULLS FIRST) AS humidity_cume_dist,
        PERCENT_RANK() OVER (ORDER BY humidity NULLS LAST) AS humidity_percent_rank,
        RANK() OVER (ORDER BY pressure NULLS FIRST) AS pressure_rank
    FROM reconciled
    WHERE reading_ts < '2023-10-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        reading_ts,
        firmware,
        device_code_group,
        MAX(pressure_rank) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS pressure_rank_max_2h,
        SUM(humidity_percent_rank) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS humidity_percent_rank_sum_15m
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    PERCENTILE_APPROX(humidity_percent_rank_sum_15m, ARRAY(0.25, 0.5, 0.75)) AS humidity_percent_rank_sum_15m_quartiles,
    MAX(reading_ts) AS ended_at,
    ROUND(AVG(humidity_percent_rank_sum_15m), 2) AS avg_humidity_percent_rank_sum_15m
FROM scored
GROUP BY DATE_TRUNC('HOUR', reading_ts), DATE_TRUNC('DAY', reading_ts)
ORDER BY hour_window;
