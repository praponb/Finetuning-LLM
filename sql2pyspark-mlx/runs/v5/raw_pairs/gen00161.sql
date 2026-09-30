WITH base AS (
    SELECT
        *,
        LENGTH(device_code) device_code_length,
        status_bits * humidity status_bits_times_humidity
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        device_code_length,
        SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
filtered AS (
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        firmware,
        device_code_length,
        session_id,
        ROUND(pressure / temp_c, 2) pressure_per_temp_c
    FROM sessions_device_readings
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 07:00:00')
      AND firmware NOT IN ('2.0.1', '1.1.0')
),
metrics AS (
    SELECT
        id,
        pressure,
        device_code_length,
        session_id,
        pressure_per_temp_c,
        AVG(device_code_length) OVER w_grp device_code_length_avg_6,
        COUNT(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) pressure_count_1h
    FROM filtered
    WINDOW w_grp AS (ORDER BY session_id, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
daily AS (
    SELECT
        CASE WHEN pressure_count_1h >= 14 THEN 'LARGE' ELSE 'SMALL' END AS pressure_count_1h_tier,
        MAX(pressure) AS max_pressure,
        COUNT(*) AS row_count
    FROM metrics
    GROUP BY CASE WHEN pressure_count_1h >= 14 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    pressure_count_1h_tier,
    max_pressure,
    row_count
FROM daily;
