WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(status_bits) AS status_bits_whole
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '1' HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    session_id,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(session_id) FILTER (WHERE device_code = 'DV-03') AS dv_03_total_session_id,
    MAX(status_bits_whole) FILTER (WHERE device_code = 'DV-04') AS dv_04_max_status_bits_whole,
    COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count,
    AVG(humidity) FILTER (WHERE device_code = 'DV-02') AS dv_02_avg_humidity,
    GROUPING(session_id) AS grouping_level
FROM sessionized
GROUP BY CUBE (session_id, DATE_TRUNC('HOUR', reading_ts))
ORDER BY dv_04_max_status_bits_whole;
