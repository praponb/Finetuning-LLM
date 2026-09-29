WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(firmware) AS firmware_upper
    FROM device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 08:00:00')
      AND TO_DATE(reading_ts) < '2025-06-04'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY temp_c DESC, id) AS row_seq,
        COUNT(status_bits) OVER (PARTITION BY device_code) AS device_code_count_status_bits,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share
    FROM cleaned
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS rn
    FROM metrics
    QUALIFY rn = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        firmware_upper,
        device_code_count_status_bits,
        pressure_share,
        SUM(device_code_count_status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS device_code_count_status_bits_sum_1h,
        AVG(device_code_count_status_bits) OVER (PARTITION BY device_code ORDER BY pressure_share, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS device_code_count_status_bits_avg_5,
        PERCENT_RANK() OVER w_ordered AS device_code_count_status_bits_percent_rank
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY device_code_count_status_bits DESC NULLS LAST)
),
metrics_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(device_code_count_status_bits, 2) OVER w_part AS prev_device_code_count_status_bits,
        AVG(pressure_share) OVER (ORDER BY status_bits, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pressure_share_avg_2_prior
    FROM enriched
    WHERE pressure_share >= 0.44
    WINDOW w_part AS (ORDER BY temp_c, id)
),
report AS (
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware_upper,
        device_code_count_status_bits,
        pressure_share,
        device_code_count_status_bits_sum_1h
    FROM metrics_device_readings
    ORDER BY temp_c NULLS LAST
)
SELECT
    id,
    humidity,
    pressure,
    status_bits,
    device_code_count_status_bits,
    pressure_share
FROM report;
