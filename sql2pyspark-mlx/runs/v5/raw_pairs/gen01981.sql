WITH kept_keys AS (
    -- Set intersection on keys
    SELECT device_code, firmware
    FROM device_readings
    INTERSECT
    SELECT device_code, firmware
    FROM device_readings
    WHERE device_code NOT IN ('DV-03', 'DV-01', 'DV-04')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    JOIN kept_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
)
-- Grouped metrics
SELECT
    device_code,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    SUM(CASE WHEN device_code = 'DV-01' THEN pressure ELSE 0 END) AS dv_01_pressure,
    COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM verified
GROUP BY device_code, DATE_TRUNC('DAY', reading_ts)
ORDER BY activity_day DESC;
