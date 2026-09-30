WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(humidity) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS pressure_avg_15m
    FROM device_readings
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    pressure,
    status_bits,
    firmware,
    pressure_avg_15m
FROM with_windows
ORDER BY pressure_avg_15m NULLS FIRST
