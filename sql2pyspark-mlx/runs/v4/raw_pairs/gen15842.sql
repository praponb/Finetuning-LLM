WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(firmware) AS firmware_lower
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id
    FROM device_readings
    WHERE pressure > 1016
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS humidity_avg_30m,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS temp_c_avg_1h
    FROM reconciled
)
-- Grouped metrics
SELECT
    device_code,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    STDDEV_SAMP(temp_c) AS temp_c_stddev,
    ROUND(AVG(humidity_avg_30m), 2) AS avg_humidity_avg_30m,
    MAX(pressure) AS max_pressure,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list
FROM metrics
GROUP BY device_code, hour_window
ORDER BY hour_window DESC;
