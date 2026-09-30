SELECT
    device_code,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    MAX(reading_ts) AS last_reading_ts,
    PERCENTILE_APPROX(humidity, 0.5) AS humidity_p50,
    PERCENTILE_APPROX(status_bits, ARRAY(0.25, 0.5, 0.75)) AS status_bits_quartiles,
    GROUPING(device_code) AS grouping_id
FROM device_readings
GROUP BY GROUPING SETS ((device_code, DATE_TRUNC('HOUR', reading_ts)), (device_code))
ORDER BY hour_window
