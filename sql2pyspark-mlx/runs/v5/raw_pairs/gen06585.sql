WITH cleaned AS (
    SELECT *, pressure - humidity AS pressure_minus_humidity
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 07:00:00'
)
SELECT device_code
FROM cleaned
EXCEPT
SELECT device_code
FROM device_readings
WHERE firmware != '1.0.3';
