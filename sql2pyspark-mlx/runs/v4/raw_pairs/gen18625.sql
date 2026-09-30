SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        humidity
    FROM device_readings
) src
PIVOT (
    AVG(humidity) AS value_sum
    FOR device_code IN ('DV-01' AS dv_01, 'DV-04' AS dv_04, 'DV-02' AS dv_02)
)
