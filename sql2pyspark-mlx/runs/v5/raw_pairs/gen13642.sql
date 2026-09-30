WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        LOWER(firmware) AS firmware_lower,
        ROUND(pressure * 0.85, 2) AS pressure_adjusted
    FROM device_readings
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 03:00:00')
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM valid
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    CASE WHEN temp_c >= 37.9 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
    (UNIX_TIMESTAMP(MAX(prev_reading_ts)) - UNIX_TIMESTAMP(MIN(prev_reading_ts))) / 60 AS duration_min,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT(*) AS row_count,
    SUM(burst_id) FILTER (WHERE device_code = 'DV-02') AS dv_02_total_burst_id,
    CORR(temp_c, pressure) AS corr_temp_c_pressure
FROM islands
GROUP BY DATE_TRUNC('HOUR', reading_ts), CASE WHEN temp_c >= 37.9 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING dv_02_total_burst_id > 0
ORDER BY temp_c_tier DESC NULLS LAST
