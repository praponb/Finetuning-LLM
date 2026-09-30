WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN device_code = 'DV-02' THEN 1 ELSE 0 END AS is_dv_02,
        pressure + humidity AS pressure_plus_humidity
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 03:00:00'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        pressure
    FROM base
) src
PIVOT (
    SUM(pressure)
    FOR device_code IN ('DV-03' AS dv_03, 'DV-04' AS dv_04)
);
