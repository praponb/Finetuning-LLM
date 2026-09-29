WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, firmware
    FROM device_readings
    EXCEPT ALL
    SELECT id, firmware
    FROM device_readings
    WHERE humidity <= 75
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    JOIN remaining_keys k ON s.id = k.id AND s.firmware = k.firmware
)
-- Multi-level aggregation
SELECT
    CASE WHEN humidity >= 80 THEN 'LARGE' ELSE 'SMALL' END AS humidity_tier,
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
    COUNT(*) AS cnt,
    ROUND(AVG(pressure), 2) AS avg_pressure,
    ROUND(AVG(status_bits), 2) AS avg_status_bits,
    MAX(reading_ts) AS ended_at,
    CORR(pressure, temp_c) AS corr_pressure_temp_c
FROM kept_rows
GROUP BY GROUPING SETS ((CASE WHEN humidity >= 80 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('DAY', reading_ts), DATE_TRUNC('HOUR', reading_ts)), (CASE WHEN humidity >= 80 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('DAY', reading_ts)), (CASE WHEN humidity >= 80 THEN 'LARGE' ELSE 'SMALL' END), ());
