WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    firmware,
    SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM with_gaps
