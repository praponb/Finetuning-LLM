WITH with_windows AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS humidity_avg_2,
        LAST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY temp_c ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity
    FROM device_readings
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity_avg_2,
    CASE WHEN device_code RLIKE '^(DV-04|DV-03)$' THEN 'CORE' WHEN device_code RLIKE '^(DV-01|DV-02)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS device_code_zone
FROM with_windows;
