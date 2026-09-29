WITH kept_keys AS (
    -- Set difference on keys
    SELECT firmware, device_code
    FROM device_readings
    EXCEPT
    SELECT firmware, device_code
    FROM device_readings
    WHERE TO_DATE(reading_ts) < '2025-09-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    JOIN kept_keys k ON s.firmware = k.firmware AND s.device_code = k.device_code
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    PERCENTILE_APPROX(pressure, ARRAY(0.25, 0.5, 0.75)) AS pressure_quartiles,
    COUNT_IF(device_code = 'DV-03') AS dv_03_events,
    MIN(pressure) AS min_pressure
FROM verified
GROUP BY CUBE (DATE_TRUNC('DAY', reading_ts))
ORDER BY min_pressure DESC;
