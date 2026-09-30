WITH with_prev AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM device_readings
),
flagged AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    device_code,
    session_id,
    COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count,
    ROUND(AVG(pressure), 2) AS avg_pressure,
    STDDEV_SAMP(pressure) AS pressure_stddev,
    COUNT_IF(device_code = 'DV-04') AS dv_04_events,
    COUNT(DISTINCT device_code) AS distinct_device_codes,
    PERCENT_RANK() OVER (PARTITION BY device_code ORDER BY STDDEV_SAMP(pressure) DESC) AS pressure_stddev_percent_rank
FROM sessionized
GROUP BY device_code, session_id
HAVING dv_03_count < 1000
ORDER BY avg_pressure DESC;
