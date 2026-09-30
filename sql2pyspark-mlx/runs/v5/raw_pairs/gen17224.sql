SELECT *
FROM (
    SELECT
        id,
        device_code,
        humidity,
        temp_c,
        pressure
    FROM device_readings
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (humidity AS HUMIDITY_VAL, temp_c AS TEMP_C_VAL, pressure AS PRESSURE_VAL)
)
