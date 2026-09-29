SELECT *
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
    reading FOR metric_name IN (temp_c, pressure, humidity)
);
