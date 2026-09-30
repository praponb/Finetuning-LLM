WITH unpivoted AS (
    SELECT *
    FROM (
        SELECT
            id,
            device_code,
            humidity,
            temp_c,
            pressure
        FROM device_readings
    ) src
    UNPIVOT (
        metric_value FOR measure IN (humidity, temp_c, pressure)
    )
)
SELECT
    device_code,
    measure,
    SUM(metric_value) AS total_metric_value,
    ROUND(SUM(metric_value) / NULLIF(COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END), 0), 2) AS metric_value_per_dv_03,
    COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END) AS dv_03_count
FROM unpivoted
GROUP BY device_code, measure
HAVING SUM(metric_value) < 1000000;
