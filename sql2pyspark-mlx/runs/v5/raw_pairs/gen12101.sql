WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        LENGTH(firmware) AS firmware_length,
        CASE WHEN pressure >= 1025 THEN 'High' WHEN pressure >= 1003 THEN 'Medium' ELSE 'Low' END AS pressure_level
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 06:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(firmware_length) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS firmware_length_avg_3,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY firmware_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure,
        LAG(firmware_length, 2, 0) OVER (ORDER BY reading_ts, id) AS prev_firmware_length
    FROM valid
),
metrics_device_readings AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        firmware_length,
        pressure_level,
        cumulative_pressure,
        LAG(cumulative_pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_cumulative_pressure,
        LAG(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_pressure,
        LEAD(pressure) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS next_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS event_seq,
        COUNT(prev_firmware_length) OVER (PARTITION BY device_code) AS device_code_count_prev_firmware_length
    FROM metrics
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(prev_pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS prev_pressure_avg_2h,
        NTILE(3) OVER (ORDER BY device_code_count_prev_firmware_length DESC, id) AS device_code_count_prev_firmware_length_bucket
    FROM metrics_device_readings
)
-- Grouped metrics
SELECT
    CASE WHEN prev_pressure_avg_2h >= 1032 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_pressure_avg_2h_tier,
    CASE WHEN device_code_count_prev_firmware_length_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS device_code_count_prev_firmware_length_bucket_tier,
    MIN(reading_ts) AS first_reading_ts,
    APPROX_PERCENTILE(cumulative_pressure, ARRAY(0.25, 0.5, 0.75)) AS cumulative_pressure_quartiles,
    MAX(reading_ts) AS last_reading_ts,
    MIN(reading_ts) AS started_at,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN prev_pressure_avg_2h >= 1032 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN device_code_count_prev_firmware_length_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN prev_pressure_avg_2h >= 1032 THEN 'PREMIUM' ELSE 'STANDARD' END), ());
