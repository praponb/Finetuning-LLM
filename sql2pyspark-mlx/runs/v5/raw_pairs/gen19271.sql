WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        COUNT(humidity) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_humidity,
        MIN(humidity) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_humidity,
        MAX(humidity) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_humidity,
        SUM(humidity) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_humidity,
        CORR(pressure, humidity) OVER (PARTITION BY device_code) AS corr_pressure_humidity
    FROM device_readings
    WHERE status_bits BETWEEN 6 AND 11
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY running_max_humidity ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_status_bits,
        MIN(status_bits) OVER (PARTITION BY device_code ORDER BY running_max_humidity ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_status_bits,
        COUNT(status_bits) OVER (PARTITION BY device_code ORDER BY running_max_humidity ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_status_bits
    FROM scored
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, last3_count_status_bits * corr_pressure_humidity AS last3_count_status_bits_times_corr_pressure_humidity
    FROM enriched
    WHERE reading_ts >= DATE '2025-09-01'
      AND running_count_humidity IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        device_code,
        temp_c,
        running_count_humidity,
        running_sum_humidity,
        last3_min_status_bits,
        last3_count_status_bits_times_corr_pressure_humidity
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY running_count_humidity ASC, id ASC) AS row_num
        FROM valid
    ) t
    WHERE row_num = 1
),
ranked_device_readings AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY running_count_humidity ASC NULLS LAST, id ASC) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(last3_count_status_bits_times_corr_pressure_humidity) OVER (PARTITION BY device_code ORDER BY running_count_humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last3_count_status_bits_times_corr_pressure_humidity,
        ROW_NUMBER() OVER (ORDER BY running_count_humidity ASC, id ASC) AS row_seq
    FROM ranked_device_readings
)
-- Window metrics over several different partitions
SELECT
    *,
    MAX(last3_count_status_bits_times_corr_pressure_humidity) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_last3_count_status_bits_times_corr_pressure_humidity,
    MIN(last3_count_status_bits_times_corr_pressure_humidity) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_last3_count_status_bits_times_corr_pressure_humidity,
    COUNT(last3_count_status_bits_times_corr_pressure_humidity) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last3_count_status_bits_times_corr_pressure_humidity,
    NTILE(4) OVER (PARTITION BY device_code ORDER BY last3_count_status_bits_times_corr_pressure_humidity ASC, id ASC) AS last3_count_status_bits_times_corr_pressure_humidity_bucket
FROM metrics;
