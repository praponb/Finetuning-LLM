WITH gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM device_readings
),
sessions_device_readings AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    new_group_flag,
    island_id
FROM sessions_device_readings
ORDER BY new_group_flag NULLS FIRST
