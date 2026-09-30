SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN status_bits >= 10 THEN 'HIGH' ELSE 'LOW' END AS status_bits_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    STDDEV_SAMP(status_bits) AS status_bits_stddev
FROM device_readings
GROUP BY ROLLUP (DATE_TRUNC('DAY', reading_ts), CASE WHEN status_bits >= 10 THEN 'HIGH' ELSE 'LOW' END);
