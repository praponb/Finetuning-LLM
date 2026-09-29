WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    id,
    reading_ts,
    temp_c,
    humidity,
    firmware,
    new_group_flag,
    session_no
FROM sessionized
ORDER BY reading_ts DESC;
