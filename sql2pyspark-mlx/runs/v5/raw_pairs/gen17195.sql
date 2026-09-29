SELECT
    id,
    device_code,
    measure,
    val
FROM (
    SELECT
        id,
        device_code,
        pressure,
        humidity,
        temp_c
    FROM device_readings
) src
UNPIVOT (
    val FOR measure IN (pressure AS PRESSURE_VAL, humidity AS HUMIDITY, temp_c AS TEMP_C_VAL)
)
