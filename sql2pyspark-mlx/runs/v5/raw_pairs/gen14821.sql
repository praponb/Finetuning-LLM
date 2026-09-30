WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        ROUND(status_bits, 2) AS status_bits_rounded
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
      AND TO_DATE(reading_ts) < '2024-01-08'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        status_bits,
        firmware,
        NTILE(3) OVER (ORDER BY status_bits_rounded, id) AS status_bits_rounded_bucket,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS pressure_avg_2,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS event_seq,
        SUM(humidity) OVER (PARTITION BY device_code) AS device_code_sum_humidity,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_bits_avg_6_prior
    FROM filtered
    WHERE humidity > 44
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(pressure_avg_2, TRUE) OVER (PARTITION BY device_code ORDER BY status_bits_rounded_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_pressure_avg_2,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY pressure_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits,
        LAG(status_bits_rounded_bucket, 1) OVER (PARTITION BY device_code ORDER BY device_code_sum_humidity, id) AS prev_status_bits_rounded_bucket
    FROM metrics
),
base AS (
    -- Keep recent, valid rows
    SELECT *, event_seq / NULLIF(status_bits_avg_6_prior, 0) AS event_seq_per_status_bits_avg_6_prior
    FROM metrics_device_readings
    WHERE device_code_sum_humidity IS NOT NULL
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN pressure_avg_2 >= 1024 THEN 'LARGE' ELSE 'SMALL' END AS pressure_avg_2_tier,
        COUNT(*) AS cnt,
        AVG(event_seq_per_status_bits_avg_6_prior) FILTER (WHERE device_code = 'DV-01') AS dv_01_avg_event_seq_per_status_bits_avg_6_prior,
        COUNT(DISTINCT firmware) AS distinct_firmwares,
        COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows
    FROM base
    GROUP BY CASE WHEN pressure_avg_2 >= 1024 THEN 'LARGE' ELSE 'SMALL' END
    HAVING dv_01_avg_event_seq_per_status_bits_avg_6_prior < 1000000
)
SELECT pressure_avg_2_tier, dv_02_rows
FROM grouped
