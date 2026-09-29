WITH scored AS (
    SELECT
        id,
        reading_ts,
        humidity,
        firmware,
        STDDEV_SAMP(status_bits) OVER (PARTITION BY device_code) AS device_code_stddev_samp_status_bits,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS status_bits_avg_2,
        LAST_VALUE(temp_c) IGNORE NULLS OVER w_ordered AS last_known_temp_c
    FROM device_readings
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT
        id,
        firmware,
        device_code_stddev_samp_status_bits,
        last_known_temp_c,
        last_known_temp_c * device_code_stddev_samp_status_bits AS last_known_temp_c_times_device_code_stddev_samp_status_bits
    FROM scored
    WHERE humidity IS NOT NULL
),
valid AS (
    SELECT *, LOWER(firmware) AS firmware_lower
    FROM filtered
    WHERE device_code_stddev_samp_status_bits IS NOT NULL
),
filtered_device_readings AS (
    SELECT
        id,
        device_code_stddev_samp_status_bits,
        last_known_temp_c,
        last_known_temp_c_times_device_code_stddev_samp_status_bits,
        COALESCE(last_known_temp_c_times_device_code_stddev_samp_status_bits, 0) AS last_known_temp_c_times_device_code_stddev_samp_status_bits_filled
    FROM valid
    WHERE device_code_stddev_samp_status_bits != 10
)
SELECT
    CASE WHEN last_known_temp_c_times_device_code_stddev_samp_status_bits >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_temp_c_times_device_code_stddev_samp_status_bits_tier,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(last_known_temp_c, 0.5) AS last_known_temp_c_p50,
    APPROX_PERCENTILE(last_known_temp_c, 0.95) AS last_known_temp_c_p95,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING_ID() AS grouping_level
FROM filtered_device_readings
GROUP BY ROLLUP (CASE WHEN last_known_temp_c_times_device_code_stddev_samp_status_bits >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END);
