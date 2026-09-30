WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(reading_ts AS DATE) AS reading_ts_day
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM base
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        reading_ts,
        firmware,
        reading_ts_day,
        prev_reading_ts,
        is_new_burst
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id) AS latest_rank
        FROM islands
    ) ranked
    WHERE latest_rank = 1
),
base_device_readings AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        firmware,
        prev_reading_ts,
        is_new_burst,
        CAST(reading_ts AS DATE) AS reading_ts_day_2
    FROM ranked
    WHERE MONTH(reading_ts_day) IN (2, 6, 8)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(firmware) AS clean_firmware
    FROM base_device_readings
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 06:00:00')
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', reading_ts_day_2) AS activity_month,
    DATE_TRUNC('QUARTER', reading_ts_day_2) AS quarter_start,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    COUNT(*) AS event_count,
    MIN(prev_reading_ts) AS first_seen,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(is_new_burst), 2) AS avg_is_new_burst
FROM cleaned
GROUP BY DATE_TRUNC('MONTH', reading_ts_day_2), DATE_TRUNC('QUARTER', reading_ts_day_2)
ORDER BY duration_min DESC NULLS LAST;
