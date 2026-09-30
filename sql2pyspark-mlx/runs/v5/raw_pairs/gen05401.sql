WITH filtered AS (
    SELECT
        *,
        CAST(temp_c AS INT) AS temp_c_int,
        TRIM(firmware) AS clean_firmware
    FROM device_readings
    WHERE humidity >= 58
)
SELECT
    id,
    device_code,
    kpi,
    metric_value
FROM (
    SELECT
        id,
        device_code,
        pressure,
        humidity
    FROM filtered
) src
UNPIVOT (
    metric_value FOR kpi IN (pressure AS PRESSURE_VAL, humidity AS HUMIDITY)
) u;
