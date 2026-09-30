WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    MAX(reading_ts) AS ended_at,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows,
    MAX(reading_ts) AS last_seen
FROM sessionized
GROUP BY DATE_TRUNC('HOUR', reading_ts), DATE_TRUNC('DAY', reading_ts)
HAVING dv_02_rows < 1
ORDER BY hour_window DESC NULLS FIRST;
