WITH filtered AS (
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        UPPER(device_code) AS device_code_upper,
        humidity - temp_c AS humidity_minus_temp_c
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 09:00:00'
      AND TO_DATE(reading_ts) >= '2023-06-01'
)
SELECT
    *,
    SUM(status_bits) OVER (ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits,
    NTILE(5) OVER (ORDER BY humidity, id) AS humidity_bucket
FROM filtered;
