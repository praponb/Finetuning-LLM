WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(firmware) firmware_length,
        LOWER(firmware) firmware_lower
    FROM device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 06:00:00')
      AND firmware = '2.0.1'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        humidity,
        pressure,
        status_bits,
        firmware,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) status_bits_bucket,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST) humidity_dense_rank,
        STDDEV_SAMP(status_bits) OVER (PARTITION BY device_code) device_code_stddev_samp_status_bits
    FROM cleaned
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(humidity_dense_rank, TRUE) OVER (ORDER BY device_code_stddev_samp_status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_humidity_dense_rank,
        AVG(humidity_dense_rank) OVER (ORDER BY status_bits, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) humidity_dense_rank_avg_3_prior
    FROM metrics
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status_bits,
        firmware,
        device_code_stddev_samp_status_bits,
        humidity_dense_rank_avg_3_prior,
        ROUND(status_bits * 1.15, 2) status_bits_scaled
    FROM metrics_device_readings
    WHERE device_code_stddev_samp_status_bits = 7
)
SELECT
    id,
    status_bits,
    firmware,
    humidity_dense_rank_avg_3_prior,
    status_bits_scaled
FROM filtered
ORDER BY status_bits NULLS LAST;
