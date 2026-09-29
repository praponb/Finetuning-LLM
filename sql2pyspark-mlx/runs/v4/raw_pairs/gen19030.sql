WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits + temp_c AS status_bits_plus_temp_c,
        ROUND(status_bits, 2) AS status_bits_rounded
    FROM device_readings
    WHERE device_code = 'DV-03'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits_plus_temp_c,
        CEIL(status_bits) AS status_bits_whole
    FROM base
    WHERE firmware IN ('1.0.3', '1.1.0', '2.0.1')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(humidity) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS humidity_avg_30m,
        COUNT(status_bits_whole) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_bits_whole_count_30m
    FROM cleaned
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, firmware
    FROM scored
    EXCEPT ALL
    SELECT id, firmware
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 7 AND 15
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id AND s.firmware = k.firmware
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CAST(humidity AS INT) AS humidity_int
    FROM kept_rows
    WHERE reading_ts > DATE '2025-09-01'
),
base_device_readings AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(status_bits) AS status_bits_whole_2
    FROM valid
    WHERE reading_ts >= TIMESTAMP '2025-09-01 10:00:00'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY humidity_int DESC NULLS LAST) AS humidity_int_rank,
        PERCENT_RANK() OVER (ORDER BY humidity_avg_30m DESC) AS humidity_avg_30m_percent_rank,
        SUM(status_bits) OVER (ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_bits_sum_1h
    FROM base_device_readings
)
SELECT
    id,
    humidity,
    pressure,
    status_bits,
    firmware,
    status_bits_plus_temp_c,
    status_bits_whole,
    status_bits_whole_count_30m,
    status_bits_whole_2
FROM with_windows
