WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        status_bits,
        firmware,
        FLOOR(status_bits) AS status_bits_whole,
        CAST(pressure AS INT) AS pressure_int
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM cleaned
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    device_code,
    CASE WHEN pressure_int >= 80 THEN 'LARGE' ELSE 'SMALL' END AS pressure_int_tier,
    COUNT(*) AS cnt,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    ROUND(SUM(session_start_flag) / NULLIF(COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END), 0), 2) AS session_start_flag_per_dv_02,
    STDDEV_SAMP(status_bits) AS status_bits_stddev,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows
FROM islands
GROUP BY device_code, CASE WHEN pressure_int >= 80 THEN 'LARGE' ELSE 'SMALL' END
HAVING dv_01_rows > 1
ORDER BY span_minutes NULLS FIRST
