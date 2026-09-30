WITH matched_keys AS (
    -- Set difference on keys
    SELECT id, device_code
    FROM device_readings
    EXCEPT
    SELECT id, device_code
    FROM device_readings
    WHERE temp_c <= 32
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM device_readings s
    INNER JOIN matched_keys k ON s.id = k.id AND s.device_code = k.device_code
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits
FROM reconciled
ORDER BY device_code DESC, reading_ts;
