WITH filtered AS (
    SELECT *, CONCAT(firmware, ' - ', device_code) AS firmware_device_code
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
)
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN temp_c >= 41.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS temp_c_tier,
    SUM(status_bits) FILTER (WHERE device_code = 'DV-02') AS dv_02_total_status_bits,
    APPROX_PERCENTILE(pressure, 0.5) AS pressure_p50,
    SUM(CASE WHEN device_code = 'DV-01' THEN humidity ELSE 0 END) AS dv_01_humidity,
    COUNT(DISTINCT device_code) AS distinct_device_codes,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(reading_ts) AS ended_at
FROM filtered
GROUP BY CUBE (DATE_TRUNC('DAY', reading_ts), CASE WHEN temp_c >= 41.3 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING dv_01_humidity >= 0
