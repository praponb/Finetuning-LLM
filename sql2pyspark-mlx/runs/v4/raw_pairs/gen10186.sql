SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows,
    COUNT_IF(device_code = 'DV-01') AS dv_01_events,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    PERCENTILE_APPROX(temp_c, 0.5) AS temp_c_p50
FROM device_readings
GROUP BY DATE_TRUNC('HOUR', reading_ts);
