WITH cleaned AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        HOUR(reading_ts) AS reading_ts_hour
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 09:00:00'
),
scored AS (
    SELECT
        *,
        MIN(reading_ts_hour) OVER (PARTITION BY device_code) AS device_code_min_reading_ts_hour,
        FIRST_VALUE(reading_ts_hour) IGNORE NULLS OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reading_ts_hour,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_bits_avg_3_prior,
        DENSE_RANK() OVER (ORDER BY pressure DESC NULLS FIRST) AS pressure_dense_rank
    FROM cleaned
),
enriched AS (
    SELECT
        *,
        MIN(status_bits_avg_3_prior) OVER (PARTITION BY device_code) AS device_code_min_status_bits_avg_3_prior,
        DENSE_RANK() OVER (ORDER BY humidity DESC NULLS LAST) AS humidity_dense_rank,
        AVG(status_bits) OVER (ORDER BY temp_c, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS status_bits_avg_3
    FROM scored
),
kept_keys AS (
    SELECT id
    FROM enriched
    EXCEPT
    SELECT id
    FROM device_readings
    WHERE reading_ts < '2024-12-11'
),
verified AS (
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.id = k.id
),
valid AS (
    SELECT
        device_code,
        temp_c,
        humidity,
        pressure,
        reading_ts_hour,
        device_code_min_status_bits_avg_3_prior,
        humidity_dense_rank,
        reading_ts_hour * device_code_min_status_bits_avg_3_prior AS reading_ts_hour_times_device_code_min_status_bits_avg_3_prior
    FROM verified
    WHERE humidity IS NOT NULL
)
SELECT
    temp_c,
    humidity,
    pressure,
    reading_ts_hour,
    humidity_dense_rank
FROM valid
ORDER BY reading_ts_hour DESC NULLS FIRST, humidity;
