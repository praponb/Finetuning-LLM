WITH cleaned AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        FLOOR(pressure) AS pressure_whole
    FROM device_readings
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 02:00:00')
      AND reading_ts >= '2025-08-11'
),
scored AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        pressure_whole,
        AVG(pressure_whole) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS pressure_whole_avg_5,
        MAX(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_bits_max_30m,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY status_bits ASC) AS status_bits_dense_rank
    FROM cleaned
),
metrics AS (
    SELECT
        id,
        temp_c,
        pressure,
        status_bits,
        pressure_whole,
        status_bits_max_30m,
        DENSE_RANK() OVER (ORDER BY pressure_whole DESC) AS pressure_whole_dense_rank,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (ORDER BY pressure_whole_avg_5 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure,
        MAX(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS pressure_max_15m
    FROM scored
),
metrics_device_readings AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY status_bits_max_30m ASC, id ASC) AS status_bits_max_30m_bucket,
        FIRST_VALUE(temp_c) IGNORE NULLS OVER (ORDER BY status_bits_max_30m ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c,
        FIRST_VALUE(status_bits_max_30m, TRUE) OVER (ORDER BY status_bits_max_30m ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_bits_max_30m
    FROM metrics
)
SELECT
    id,
    metric_name,
    val
FROM (
    SELECT
        id,
        temp_c,
        first_known_pressure,
        pressure,
        first_known_temp_c
    FROM metrics_device_readings
) src
UNPIVOT (
    val FOR metric_name IN (temp_c, first_known_pressure, pressure, first_known_temp_c)
);
