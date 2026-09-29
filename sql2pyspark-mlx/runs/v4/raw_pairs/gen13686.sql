SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    COUNT(*) AS cnt,
    STDDEV_SAMP(temp_c) AS temp_c_stddev,
    CORR(pressure, status_bits) AS corr_pressure_status_bits,
    PERCENTILE_APPROX(temp_c, ARRAY(0.25, 0.5, 0.75)) AS temp_c_quartiles,
    MIN(reading_ts) AS first_seen,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes
FROM device_readings
GROUP BY hour_window
HAVING cnt < 1
ORDER BY corr_pressure_status_bits DESC;
