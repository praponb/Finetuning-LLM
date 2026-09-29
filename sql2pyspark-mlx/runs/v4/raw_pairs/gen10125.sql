WITH metrics_long AS (
    -- Native SQL UNPIVOT (wide to long)
    SELECT *
    FROM (
        SELECT
            id,
            device_code,
            humidity,
            pressure,
            temp_c
        FROM device_readings
    ) src
    UNPIVOT INCLUDE NULLS (
        metric_value FOR measure IN (humidity, pressure, temp_c)
    )
)
-- Multi-level aggregation
SELECT
    CASE WHEN metric_value >= 86 THEN 'LARGE' ELSE 'SMALL' END AS metric_value_tier,
    MAX(metric_value) FILTER (WHERE measure = 'temp_c') AS temp_c_max_metric_value,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    APPROX_PERCENTILE(metric_value, 0.95) AS metric_value_p95,
    MAX(metric_value) FILTER (WHERE device_code = 'DV-03') AS dv_03_max_metric_value
FROM metrics_long
GROUP BY metric_value_tier
ORDER BY temp_c_max_metric_value ASC NULLS LAST
