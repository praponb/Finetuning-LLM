WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(pressure * 0.85, 2) AS pressure_adjusted,
        CEIL(status_bits) AS status_bits_whole
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_bits_whole >= 65 THEN 'LARGE' ELSE 'SMALL' END AS status_bits_whole_tier,
    CASE WHEN pressure_adjusted >= 83 THEN 'LARGE' ELSE 'SMALL' END AS pressure_adjusted_tier,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    SUM(humidity) FILTER (WHERE device_code = 'DV-03') AS dv_03_total_humidity,
    MAX(reading_ts) AS last_seen,
    MIN(reading_ts) AS started_at
FROM sessionized
GROUP BY CASE WHEN status_bits_whole >= 65 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN pressure_adjusted >= 83 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('DAY', reading_ts)
HAVING SUM(humidity) FILTER (WHERE device_code = 'DV-03') > 0
ORDER BY activity_day DESC NULLS FIRST;
