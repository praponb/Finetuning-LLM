SELECT *
FROM (
    SELECT
        id,
        device_code,
        humidity,
        pressure,
        temp_c
    FROM device_readings
) src
UNPIVOT (
    reading FOR kpi IN (humidity, pressure, temp_c)
);
