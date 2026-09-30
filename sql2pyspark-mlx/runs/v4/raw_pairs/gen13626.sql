WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        WEEKOFYEAR(reading_ts) reading_ts_weekofyear,
        ROUND(pressure * 1.1, 2) adjusted_pressure
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        reading_ts_weekofyear,
        adjusted_pressure,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_pressure,
        NTILE(4) OVER (PARTITION BY device_code ORDER BY reading_ts_weekofyear, id) reading_ts_weekofyear_bucket,
        LAG(pressure, 1) OVER (PARTITION BY device_code ORDER BY pressure, id) prev_pressure,
        NTILE(3) OVER (PARTITION BY device_code ORDER BY humidity DESC, id) humidity_bucket,
        LEAD(humidity, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) next_humidity
    FROM base
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY prev_pressure DESC NULLS LAST, id) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        firmware,
        device_code,
        temp_c
    FROM latest
) src
PIVOT (
    AVG(temp_c) AS value_sum,
    COUNT(1) AS n
    FOR device_code IN ('DV-03' AS dv_03, 'DV-02' AS dv_02, 'DV-01' AS dv_01)
);
