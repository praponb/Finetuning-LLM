SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        humidity
    FROM device_readings
) src
PIVOT (
    MAX(humidity)
    FOR device_code IN ('DV-03' AS dv_03, 'DV-04' AS dv_04)
)
