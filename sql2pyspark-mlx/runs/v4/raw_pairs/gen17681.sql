SELECT
    CASE WHEN temp_c >= 37.4 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
    CORR(pressure, status_bits) AS corr_pressure_status_bits,
    BIT_AND(status_bits) AS status_bits_bit_and,
    APPROX_PERCENTILE(pressure, 0.95) AS pressure_p95,
    SUM(CASE WHEN device_code = 'DV-04' THEN temp_c ELSE 0 END) AS dv_04_temp_c,
    SUM(CASE WHEN device_code = 'DV-03' THEN humidity ELSE 0 END) AS dv_03_humidity,
    COUNT(DISTINCT id) AS distinct_ids
FROM device_readings
GROUP BY CASE WHEN temp_c >= 37.4 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING corr_pressure_status_bits < 0
