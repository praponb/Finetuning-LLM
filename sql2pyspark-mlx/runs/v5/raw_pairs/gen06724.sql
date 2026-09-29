WITH base AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        firmware,
        humidity - pressure humidity_minus_pressure,
        SUBSTRING(device_code, 1, 3) device_code_prefix
    FROM device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 01:00:00')
),
cleaned AS (
    SELECT *, CASE WHEN device_code = 'DV-01' THEN 1 ELSE 0 END is_dv_01
    FROM base
    WHERE firmware = '2.0.1'
),
base_device_readings AS (
    SELECT *, humidity_minus_pressure * humidity humidity_minus_pressure_times_humidity
    FROM cleaned
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 04:00:00')
),
with_windows AS (
    SELECT
        *,
        LAG(humidity_minus_pressure, 2) OVER (PARTITION BY device_code ORDER BY is_dv_01, id) prev_humidity_minus_pressure,
        AVG(humidity_minus_pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) humidity_minus_pressure_avg_2h,
        NTILE(5) OVER (ORDER BY is_dv_01 DESC, id) is_dv_01_bucket,
        MIN(humidity_minus_pressure_times_humidity) OVER (PARTITION BY device_code) device_code_min_humidity_minus_pressure_times_humidity
    FROM base_device_readings
),
scored AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY humidity_minus_pressure_avg_2h DESC NULLS FIRST) humidity_minus_pressure_avg_2h_cume_dist,
        MAX(is_dv_01) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) is_dv_01_max_1h
    FROM with_windows
    WHERE firmware IN ('2.0.1', '1.1.0', '1.0.3')
)
SELECT
    CASE WHEN humidity_minus_pressure_times_humidity >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS humidity_minus_pressure_times_humidity_tier,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows,
    MAX(reading_ts) AS last_seen,
    COUNT(*) FILTER (WHERE device_code = 'DV-02') AS dv_02_rows_2,
    MIN(reading_ts) AS first_seen
FROM scored
GROUP BY humidity_minus_pressure_times_humidity_tier
HAVING COUNT(*) FILTER (WHERE device_code = 'DV-02') > 1
ORDER BY dv_02_rows DESC NULLS LAST;
