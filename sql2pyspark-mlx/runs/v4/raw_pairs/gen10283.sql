WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT firmware, id
    FROM device_readings
    INTERSECT
    SELECT firmware, id
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 9 AND 16
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    INNER JOIN remaining_keys k ON s.firmware = k.firmware AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    device_code,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    STDDEV_SAMP(pressure) AS pressure_stddev,
    COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count
FROM verified
GROUP BY device_code, DATE_TRUNC('HOUR', reading_ts)
HAVING dv_04_count > 1;
