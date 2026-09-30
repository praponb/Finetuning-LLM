WITH flagged AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    device_code,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    is_new_burst,
    session_id
FROM islands
ORDER BY device_code DESC NULLS LAST, firmware DESC
