SELECT
    device_code,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    STDDEV_SAMP(humidity) AS humidity_stddev,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    MAX(status_bits) FILTER (WHERE device_code = 'DV-02') AS dv_02_max_status_bits,
    MAX(reading_ts) AS last_seen,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events_2,
    GROUPING(device_code) AS grouping_id
FROM device_readings
GROUP BY GROUPING SETS ((device_code, DATE_TRUNC('HOUR', reading_ts)), (device_code))
HAVING STDDEV_SAMP(humidity) >= 0
