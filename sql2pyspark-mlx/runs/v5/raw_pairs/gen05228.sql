WITH cleaned AS (
    SELECT *, GREATEST(status_bits, temp_c) AS greatest_status_bits_temp_c
    FROM device_readings
    WHERE pressure > 993
),
kept_keys AS (
    SELECT id, device_code
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id, device_code
    FROM device_readings
    WHERE humidity <= 62
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id AND s.device_code = k.device_code
),
base AS (
    SELECT *, CASE WHEN device_code = 'DV-01' THEN 1 ELSE 0 END AS is_dv_01
    FROM reconciled
    WHERE reading_ts > TIMESTAMP '2025-09-01 01:00:00'
),
enriched AS (
    SELECT
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        greatest_status_bits_temp_c,
        is_dv_01,
        MAX(greatest_status_bits_temp_c) OVER w_part AS device_code_max_greatest_status_bits_temp_c,
        SUM(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS pressure_sum_2h,
        AVG(pressure) OVER w_part AS device_code_avg_pressure,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share,
        COUNT(pressure) OVER w_running AS pressure_count_2h
    FROM base
    WINDOW w_part AS (PARTITION BY device_code), w_running AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN device_code_max_greatest_status_bits_temp_c >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END AS device_code_max_greatest_status_bits_temp_c_tier,
    CASE WHEN humidity >= 81 THEN 'HIGH' ELSE 'LOW' END AS humidity_tier,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS duration_min,
    ROUND(AVG(is_dv_01), 2) AS avg_is_dv_01,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    STDDEV_SAMP(temp_c) AS temp_c_stddev,
    ROUND(AVG(temp_c), 2) AS avg_temp_c,
    APPROX_PERCENTILE(temp_c, 0.9) AS temp_c_p90
FROM enriched
GROUP BY CASE WHEN device_code_max_greatest_status_bits_temp_c >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN humidity >= 81 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_is_dv_01 DESC;
