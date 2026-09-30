WITH enriched AS (
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        status_bits,
        AVG(humidity) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS humidity_avg_1h,
        COUNT(pressure) OVER (PARTITION BY device_code) AS device_code_count_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS seq_no,
        SUM(temp_c) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c
    FROM device_readings
)
SELECT
    id,
    temp_c,
    pressure,
    status_bits,
    humidity_avg_1h,
    device_code_count_pressure,
    seq_no
FROM enriched
ORDER BY humidity_avg_1h DESC NULLS LAST
