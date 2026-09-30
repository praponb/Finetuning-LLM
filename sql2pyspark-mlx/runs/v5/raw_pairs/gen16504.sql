WITH long_format AS (
    -- Native SQL UNPIVOT (wide to long)
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
        value FOR kpi IN (humidity AS HUMIDITY_VAL, temp_c, pressure AS PRESSURE_VAL)
    )
)
-- Grouped metrics
SELECT
    kpi,
    MAX(value) AS max_value,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE device_code = 'DV-04') AS dv_04_rows,
    MAX(value) AS max_value_2,
    COUNT(DISTINCT kpi) AS distinct_kpis,
    SORT_ARRAY(COLLECT_SET(kpi)) AS kpi_list
FROM long_format
GROUP BY 1
ORDER BY cnt
