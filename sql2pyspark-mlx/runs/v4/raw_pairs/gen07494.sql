SELECT
    device_code,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    SUM(CASE WHEN device_code = 'DV-03' THEN status_bits ELSE 0 END) AS dv_03_status_bits,
    COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) AS dv_01_count,
    MIN(reading_ts) AS first_seen,
    GROUPING_ID() AS grouping_level
FROM device_readings
GROUP BY GROUPING SETS ((device_code, DATE_TRUNC('DAY', reading_ts)), (device_code), ())
HAVING COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) < 1
ORDER BY activity_day NULLS LAST;
