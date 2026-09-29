WITH metrics AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY status_bits, id) AS status_bits_bucket,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY humidity, id) AS humidity_bucket
    FROM device_readings
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_bucket DESC NULLS FIRST, id) AS dedup_rank
        FROM metrics
    ) ranked
    WHERE dedup_rank = 1
),
scored AS (
    SELECT
        *,
        LAST_VALUE(humidity_bucket, TRUE) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity_bucket,
        RANK() OVER w_hist AS status_bits_bucket_rank,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share,
        NTILE(3) OVER w_part AS humidity_bucket_2,
        FIRST_VALUE(pressure) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure
    FROM deduped
    WINDOW w_hist AS (ORDER BY status_bits_bucket NULLS FIRST), w_part AS (PARTITION BY device_code ORDER BY humidity, id)
)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        first_known_pressure,
        humidity,
        temp_c,
        pressure
    FROM scored
) src
UNPIVOT (
    reading FOR kpi IN (first_known_pressure AS FIRST_KNOWN_PRESSURE_VAL, humidity, temp_c AS TEMP_C, pressure)
)
