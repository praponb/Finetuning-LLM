WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CAST(humidity AS INT) AS humidity_int
    FROM device_readings
    WHERE humidity BETWEEN 36 AND 45
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        device_code,
        pressure,
        temp_c,
        humidity
    FROM valid
) src
UNPIVOT (
    reading FOR measure IN (pressure, temp_c, humidity)
);
