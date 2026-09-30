SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        status_bits
    FROM device_readings
) src
PIVOT (
    SUM(status_bits) AS amount
    FOR device_code IN ('DV-03' AS dv_03, 'DV-01' AS dv_01, 'DV-02' AS dv_02)
)
