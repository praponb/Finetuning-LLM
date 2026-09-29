WITH kept_keys AS (
    SELECT firmware, id
    FROM device_readings
    EXCEPT
    SELECT firmware, id
    FROM device_readings
    WHERE firmware NOT IN ('1.1.0', '2.0.1')
),
verified AS (
    SELECT s.*
    FROM device_readings s
    INNER JOIN kept_keys k ON s.firmware = k.firmware AND s.id = k.id
),
valid AS (
    SELECT
        *,
        HOUR(reading_ts) AS reading_ts_hour,
        CONCAT_WS(',', firmware, device_code) AS firmware_device_code_key
    FROM verified
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 11:00:00')
),
valid_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        reading_ts_hour,
        firmware_device_code_key,
        CASE WHEN temp_c >= 35.7 THEN 'High' WHEN temp_c >= 24.6 THEN 'Medium' ELSE 'Low' END AS temp_c_tier
    FROM valid
    WHERE reading_ts > DATE '2025-09-01'
)
SELECT *
FROM (
    SELECT
        device_code,
        temp_c,
        pressure,
        humidity
    FROM valid_device_readings
) src
UNPIVOT INCLUDE NULLS (
    val FOR measure IN (temp_c, pressure, humidity)
)
