SELECT
    id,
    device_code,
    metric,
    reading
FROM (
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        humidity
    FROM device_readings
) src
UNPIVOT (
    reading FOR metric IN (temp_c, pressure, humidity)
) unp;
