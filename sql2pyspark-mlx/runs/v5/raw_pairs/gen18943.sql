WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        humidity,
        status_bits,
        firmware,
        HOUR(reading_ts) AS reading_ts_hour,
        LOWER(firmware) AS firmware_lower
    FROM device_readings
    WHERE pressure < 1019
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        humidity,
        firmware
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts_hour DESC NULLS FIRST, id ASC) = 1
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(firmware) AS firmware_length
    FROM deduped
    WHERE firmware <> '1.1.0'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        firmware,
        firmware_length,
        MIN(firmware_length) OVER (ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_firmware_length,
        SUM(firmware_length) OVER (ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_firmware_length,
        COUNT(firmware_length) OVER (ORDER BY humidity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_firmware_length,
        LEAD(firmware_length, 1, 0) OVER (ORDER BY humidity ASC, id ASC) AS next_firmware_length
    FROM filtered_device_readings
),
with_windows_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY running_count_firmware_length ASC NULLS LAST) AS running_count_firmware_length_rank,
        AVG(running_min_firmware_length) OVER (ORDER BY running_sum_firmware_length ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_min_firmware_length_avg_2
    FROM with_windows
    WHERE running_sum_firmware_length > 250
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        firmware,
        firmware_length,
        running_min_firmware_length,
        running_sum_firmware_length,
        running_count_firmware_length,
        next_firmware_length,
        running_min_firmware_length_avg_2,
        running_min_firmware_length - running_count_firmware_length AS running_min_firmware_length_minus_running_count_firmware_length
    FROM with_windows_device_readings
    WHERE firmware = '1.1.0'
),
valid_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, running_min_firmware_length - running_min_firmware_length_minus_running_count_firmware_length AS running_min_firmware_length_minus_running_min_firmware_length_minus_running_count_firmware_length
    FROM valid
    WHERE running_sum_firmware_length <= 140
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_min_firmware_length >= 72 THEN 'HIGH' ELSE 'LOW' END AS running_min_firmware_length_tier,
    PERCENTILE_APPROX(running_min_firmware_length_minus_running_count_firmware_length, ARRAY(0.25, 0.5, 0.75)) AS running_min_firmware_length_minus_running_count_firmware_length_quartiles,
    APPROX_PERCENTILE(running_sum_firmware_length, 0.5) AS running_sum_firmware_length_p50,
    COUNT(*) AS cnt
FROM valid_device_readings
GROUP BY 1
HAVING COUNT(*) >= 1
ORDER BY running_sum_firmware_length_p50 ASC NULLS LAST;
