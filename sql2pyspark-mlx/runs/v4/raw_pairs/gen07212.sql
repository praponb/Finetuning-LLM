WITH filtered AS (
    SELECT *, CAST(reading_ts AS DATE) AS reading_ts_day
    FROM device_readings
    WHERE reading_ts >= TO_DATE('2025-09-01')
),
kept_keys AS (
    SELECT device_code, firmware
    FROM filtered
    EXCEPT
    SELECT device_code, firmware
    FROM device_readings
    WHERE pressure IS NULL
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.device_code = k.device_code AND s.firmware = k.firmware
),
with_windows AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        reading_ts_day,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY pressure NULLS LAST) AS pressure_dense_rank,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_bits_sum_2h,
        CORR(pressure, temp_c) OVER (PARTITION BY device_code) AS corr_pressure_temp_c
    FROM reconciled
),
scored AS (
    SELECT
        *,
        COUNT(status_bits_sum_2h) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_bits_sum_2h_count_2h,
        SUM(humidity) OVER (PARTITION BY device_code) AS device_code_sum_humidity,
        SUM(corr_pressure_temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS corr_pressure_temp_c_sum_30m
    FROM with_windows
)
SELECT
    id,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    reading_ts_day,
    pressure_dense_rank,
    status_bits_sum_2h
FROM scored;
