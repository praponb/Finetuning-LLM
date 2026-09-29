SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        temp_c
    FROM device_readings
) src
PIVOT (
    SUM(temp_c) AS amount
    FOR device_code IN ('DV-04' AS dv_04, 'DV-01' AS dv_01)
);
