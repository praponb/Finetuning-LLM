WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(device_code) AS device_code_upper
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND firmware IN ('2.0.1', '1.0.3')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        temp_c
    FROM cleaned
) src
PIVOT (
    MAX(temp_c)
    FOR device_code IN ('DV-01' AS dv_01, 'DV-03' AS dv_03, 'DV-02' AS dv_02)
);
