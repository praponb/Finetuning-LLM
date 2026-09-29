WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Grouped metrics
SELECT
    device_code,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(humidity) AS avg_humidity,
    GROUPING(device_code) AS grouping_level
FROM islands
GROUP BY GROUPING SETS ((device_code, DATE_TRUNC('HOUR', reading_ts)), (device_code), ())
ORDER BY hour_window NULLS LAST;
