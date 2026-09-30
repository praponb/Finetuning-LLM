WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(device_code) device_code_lower
    FROM device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 08:00:00')
      AND device_code != 'DV-04'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM base
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    device_code,
    ROUND(SUM(humidity) / NULLIF(COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END), 0), 2) AS humidity_per_dv_02,
    MIN(prev_reading_ts) AS first_prev_reading_ts,
    SUM(CASE WHEN device_code = 'DV-04' THEN island_id ELSE 0 END) AS dv_04_island_id
FROM islands
GROUP BY device_code;
