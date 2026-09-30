WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN pressure >= 1031 THEN 'High' WHEN pressure >= 989 THEN 'Medium' ELSE 'Low' END AS pressure_tier,
        UPPER(device_code) AS device_code_upper
    FROM device_readings
    WHERE humidity > 48
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    APPROX_PERCENTILE(pressure, 0.95) AS pressure_p95,
    COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min
FROM valid
GROUP BY activity_day
HAVING dv_04_count > 1;
