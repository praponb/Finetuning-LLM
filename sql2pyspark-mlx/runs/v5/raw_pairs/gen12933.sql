WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        LOWER(device_code) AS device_code_lower
    FROM device_readings
    WHERE firmware = '1.1.0'
),
with_prev AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM filtered
),
with_gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    device_code,
    DATE_TRUNC('WEEK', prev_reading_ts) AS week_start,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows
FROM sessionized
GROUP BY 1, 2
