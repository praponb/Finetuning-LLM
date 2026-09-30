WITH long_format AS (
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
    UNPIVOT (
        reading FOR metric IN (humidity, pressure, temp_c)
    )
)
SELECT
    device_code,
    metric,
    PERCENTILE_APPROX(reading, ARRAY(0.25, 0.5, 0.75)) AS reading_quartiles,
    MAX(reading) FILTER (WHERE device_code = 'DV-02') AS dv_02_max_reading,
    ROUND(AVG(reading), 2) AS avg_reading,
    RANK() OVER (PARTITION BY device_code ORDER BY ROUND(AVG(reading), 2) DESC) AS avg_reading_rank
FROM long_format
GROUP BY device_code, metric
ORDER BY device_code DESC NULLS FIRST;
