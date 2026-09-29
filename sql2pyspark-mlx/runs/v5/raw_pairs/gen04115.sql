SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN humidity >= 71 THEN 'HIGH' ELSE 'LOW' END AS humidity_tier,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows,
    CORR(status_bits, pressure) AS corr_status_bits_pressure,
    PERCENTILE_APPROX(pressure, ARRAY(0.25, 0.5, 0.75)) AS pressure_quartiles
FROM device_readings
GROUP BY 1, 2
HAVING CORR(status_bits, pressure) < 1000000;
