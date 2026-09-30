WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(temp_c * 1.15, 2) AS temp_c_adjusted
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
      AND reading_ts >= '2023-04-01'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        status_bits
    FROM filtered
) src
PIVOT (
    AVG(status_bits) AS total
    FOR device_code IN ('DV-04' AS dv_04, 'DV-03' AS dv_03)
)
