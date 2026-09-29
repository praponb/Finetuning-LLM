WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(status_bits AS STRING) AS status_bits_str,
        LOWER(firmware) AS firmware_lower
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) AS latest_rank
        FROM cleaned
    ) ranked
    WHERE latest_rank = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT device_code, firmware
    FROM deduped
    INTERSECT
    SELECT device_code, firmware
    FROM device_readings
    WHERE device_code = 'DV-03'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN kept_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        pressure,
        status_bits,
        status_bits_str,
        firmware_lower,
        DENSE_RANK() OVER w_grp AS pressure_dense_rank,
        SUM(humidity) OVER (PARTITION BY device_code) AS device_code_sum_humidity,
        COUNT(temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS temp_c_count_1h,
        MAX(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS temp_c_max_15m,
        MAX(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_bits_max_1h
    FROM verified
    WINDOW w_grp AS (ORDER BY pressure NULLS FIRST)
),
cleaned_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        pressure,
        status_bits,
        status_bits_str,
        firmware_lower,
        pressure_dense_rank,
        device_code_sum_humidity,
        temp_c_count_1h,
        temp_c_max_15m,
        COALESCE(pressure, 0) AS pressure_filled
    FROM scored
    WHERE pressure > 1020
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        pressure,
        status_bits,
        pressure_dense_rank,
        temp_c_count_1h,
        temp_c_max_15m,
        ROUND(temp_c_count_1h, 0) AS temp_c_count_1h_rounded
    FROM cleaned_device_readings
    WHERE temp_c_max_15m IS NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY pressure NULLS LAST) AS pressure_rank,
        CUME_DIST() OVER (ORDER BY temp_c_count_1h DESC NULLS FIRST) AS temp_c_count_1h_cume_dist,
        CUME_DIST() OVER (ORDER BY pressure DESC NULLS FIRST) AS pressure_cume_dist
    FROM base
)
-- Ranking and gap features
SELECT
    *,
    CUME_DIST() OVER (ORDER BY pressure_rank NULLS FIRST) AS pressure_rank_cume_dist,
    DENSE_RANK() OVER (ORDER BY temp_c_max_15m DESC NULLS FIRST) AS temp_c_max_15m_dense_rank,
    DENSE_RANK() OVER (ORDER BY temp_c_max_15m DESC NULLS FIRST) AS temp_c_max_15m_dense_rank_2,
    DENSE_RANK() OVER (ORDER BY pressure_rank DESC NULLS LAST) AS pressure_rank_dense_rank,
    RANK() OVER (ORDER BY temp_c_count_1h_cume_dist DESC NULLS FIRST) AS temp_c_count_1h_cume_dist_rank
FROM with_windows;
