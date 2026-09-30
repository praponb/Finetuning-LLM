WITH base AS (
    SELECT
        *,
        TRIM(device_code) AS clean_device_code,
        CASE WHEN device_code IN ('DV-02', 'DV-04') THEN 'Priority' ELSE 'Other' END AS device_code_group
    FROM device_readings
    WHERE humidity < 51
)
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        status_bits
    FROM base
) src
PIVOT (
    AVG(status_bits) AS total,
    COUNT(1) AS n
    FOR device_code IN ('DV-03' AS dv_03, 'DV-01' AS dv_01, 'DV-04' AS dv_04)
);
