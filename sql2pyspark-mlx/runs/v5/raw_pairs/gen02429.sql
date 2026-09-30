WITH metrics_long AS (
    -- Native SQL UNPIVOT (wide to long)
    SELECT *
    FROM (
        SELECT
            id,
            device_code,
            temp_c,
            pressure,
            humidity
        FROM device_readings
    ) src
    UNPIVOT (
        val FOR measure IN (temp_c, pressure, humidity)
    )
)
-- Aggregate with conditional metrics
SELECT
    measure,
    CASE WHEN val >= 86 THEN 'LARGE' ELSE 'SMALL' END AS val_tier,
    ROUND(AVG(val), 2) AS avg_val,
    PERCENTILE_APPROX(val, 0.5) AS val_p50,
    SUM(CASE WHEN device_code = 'DV-04' THEN val ELSE 0 END) AS dv_04_val,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT_IF(measure = 'temp_c') AS temp_c_events,
    SIZE(COLLECT_SET(measure)) AS num_measures,
    RANK() OVER (PARTITION BY measure ORDER BY PERCENTILE_APPROX(val, 0.5) DESC) AS val_p50_rank
FROM metrics_long
GROUP BY measure, CASE WHEN val >= 86 THEN 'LARGE' ELSE 'SMALL' END
HAVING temp_c_events > 1
ORDER BY dv_04_val DESC;
