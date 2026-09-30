SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    NTILE(10) OVER (ORDER BY humidity ASC, id ASC) AS humidity_bucket,
    NTILE(5) OVER (PARTITION BY device_code ORDER BY temp_c DESC, id ASC) AS temp_c_bucket
FROM device_readings
