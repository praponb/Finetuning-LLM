WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('/', firmware, device_code) AS firmware_device_code_key
    FROM sessions_device_readings
    WHERE is_new_burst <= 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY is_new_burst, id) AS is_new_burst_bucket,
        LAST_VALUE(humidity) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share
    FROM base
)
-- Grouped metrics
SELECT
    CASE WHEN is_new_burst >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_new_burst_tier,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    APPROX_PERCENTILE(session_no, ARRAY(0.25, 0.5, 0.75)) AS session_no_quartiles,
    APPROX_PERCENTILE(is_new_burst_bucket, 0.9) AS is_new_burst_bucket_p90,
    ROUND(SUM(is_new_burst) / NULLIF(COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END), 0), 2) AS is_new_burst_per_dv_02,
    COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
    MAX(last_known_humidity) FILTER (WHERE device_code = 'DV-01') AS dv_01_max_last_known_humidity
FROM with_windows
GROUP BY 1, 2;
