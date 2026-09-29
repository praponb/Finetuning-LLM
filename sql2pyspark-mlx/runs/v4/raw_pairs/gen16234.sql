SELECT
    CASE WHEN status_bits >= 10 THEN 'LARGE' ELSE 'SMALL' END AS status_bits_tier,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(temp_c, 0.5) AS temp_c_p50,
    ROUND(SUM(humidity) / NULLIF(COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END), 0), 2) AS humidity_per_dv_04
FROM device_readings
GROUP BY CASE WHEN status_bits >= 10 THEN 'LARGE' ELSE 'SMALL' END
