WITH with_prev AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
with_gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('QUARTER', prev_reading_ts) AS quarter_start,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END), 0), 2) AS session_id_per_dv_04,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    PERCENTILE_APPROX(humidity, ARRAY(0.25, 0.5, 0.75)) AS humidity_quartiles,
    COUNT_IF(device_code = 'DV-02') AS dv_02_events
FROM islands
GROUP BY quarter_start
ORDER BY session_id_per_dv_04 DESC NULLS FIRST
