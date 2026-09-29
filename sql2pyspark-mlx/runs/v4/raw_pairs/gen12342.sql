WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(firmware, 1, 4) firmware_prefix,
        temp_c * status_bits temp_c_times_status_bits
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
)
-- Window metrics over several different partitions
SELECT
    *,
    SUM(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) pressure_sum_30m,
    NTILE(4) OVER (ORDER BY pressure, id) pressure_bucket
FROM filtered;
