WITH gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    status_bits,
    SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM gaps;
