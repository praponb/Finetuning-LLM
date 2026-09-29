WITH gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM device_readings
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    session_start_flag,
    island_id
FROM sessionized
