WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(temp_c, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_temp_c,
        LAG(status_bits, 2) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_status_bits,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share
    FROM device_readings
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    firmware,
    prev_status_bits,
    temp_c_share
FROM metrics
ORDER BY temp_c;
