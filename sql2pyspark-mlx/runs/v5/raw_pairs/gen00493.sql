SELECT
    device_code,
    COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    STDDEV_SAMP(status_bits) AS status_bits_stddev,
    MIN(reading_ts) AS started_at,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    APPROX_PERCENTILE(status_bits, ARRAY(0.25, 0.5, 0.75)) AS status_bits_quartiles
FROM device_readings
GROUP BY device_code;
