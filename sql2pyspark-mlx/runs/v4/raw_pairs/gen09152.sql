WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        status_bits,
        firmware,
        pressure + status_bits AS pressure_plus_status_bits
    FROM device_readings
    WHERE pressure BETWEEN 1022 AND 1026
),
gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    session_no,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    COUNT(CASE WHEN device_code = 'DV-02' THEN 1 END) AS dv_02_count,
    APPROX_PERCENTILE(session_no, ARRAY(0.25, 0.5, 0.75)) AS session_no_quartiles,
    SUM(CASE WHEN device_code = 'DV-04' THEN temp_c ELSE 0 END) AS dv_04_temp_c,
    MIN(reading_ts) AS started_at
FROM islands
GROUP BY GROUPING SETS ((DATE_TRUNC('HOUR', reading_ts), session_no), (DATE_TRUNC('HOUR', reading_ts)), ())
HAVING (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 > 0
ORDER BY duration_min DESC NULLS LAST
