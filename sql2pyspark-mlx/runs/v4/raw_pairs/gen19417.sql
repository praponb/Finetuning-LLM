WITH matched_keys AS (
    -- Set intersection on keys
    SELECT firmware, id
    FROM device_readings
    INTERSECT
    SELECT firmware, id
    FROM device_readings
    WHERE pressure BETWEEN 1010 AND 1021
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    JOIN matched_keys k ON s.firmware = k.firmware AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_bits >= 10 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_bits_tier,
    COUNT(*) AS num_rows,
    MAX(reading_ts) AS last_seen,
    CORR(humidity, temp_c) AS corr_humidity_temp_c
FROM verified
GROUP BY CASE WHEN status_bits >= 10 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY num_rows
