WITH kept_keys AS (
    SELECT id, firmware
    FROM device_readings
    INTERSECT ALL
    SELECT id, firmware
    FROM device_readings
    WHERE status_bits BETWEEN 4 AND 11
),
kept_rows AS (
    SELECT s.*
    FROM device_readings s
    INNER JOIN kept_keys k ON s.id = k.id AND s.firmware = k.firmware
)
SELECT
    device_code,
    STDDEV_SAMP(pressure) AS pressure_stddev,
    ROUND(SUM(pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS pressure_per_dv_01
FROM kept_rows
GROUP BY 1;
