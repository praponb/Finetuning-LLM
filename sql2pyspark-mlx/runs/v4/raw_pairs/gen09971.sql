WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        status_bits,
        firmware,
        FLOOR(temp_c) AS temp_c_whole
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 14:00:00'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(device_code) AS device_code_length,
        LOWER(firmware) AS firmware_lower
    FROM filtered
    WHERE reading_ts > TIMESTAMP '2025-09-01 09:00:00'
      AND HOUR(reading_ts) BETWEEN 7 AND 11
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(device_code_length, 1) OVER (PARTITION BY device_code ORDER BY status_bits ASC, id ASC) AS prev_device_code_length,
        ROW_NUMBER() OVER (ORDER BY status_bits ASC, id ASC) AS seq_no,
        AVG(temp_c_whole) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS temp_c_whole_avg_2h
    FROM valid
)
SELECT
    id,
    reading_ts,
    status_bits,
    firmware,
    device_code_length,
    firmware_lower,
    prev_device_code_length,
    seq_no
FROM metrics
ORDER BY device_code_length DESC NULLS LAST;
