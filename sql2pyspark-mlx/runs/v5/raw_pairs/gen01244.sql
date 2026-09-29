WITH enriched AS (
    SELECT
        *,
        AVG(pressure) OVER w_hist AS pressure_avg_6,
        ROW_NUMBER() OVER (ORDER BY reading_ts, id) AS row_seq,
        CORR(humidity, pressure) OVER (PARTITION BY device_code) AS corr_humidity_pressure
    FROM device_readings
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    firmware,
    pressure_avg_6,
    row_seq,
    corr_humidity_pressure,
    CASE WHEN device_code RLIKE '^(DV-02|DV-04)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS device_code_zone
FROM enriched
ORDER BY reading_ts DESC, corr_humidity_pressure DESC
