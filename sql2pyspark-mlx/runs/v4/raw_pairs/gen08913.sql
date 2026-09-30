WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC) AS rank_in_group
        FROM device_readings
    ) ranked
    WHERE rank_in_group = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        CAST(pressure AS INT) AS pressure_int
    FROM ranked
    WHERE reading_ts > DATE '2025-09-01'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity NULLS LAST, id) = 1
),
daily AS (
    -- Grouped metrics
    SELECT
        device_code,
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        MIN(reading_ts) AS first_reading_ts,
        COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
        AVG(pressure_int) FILTER (WHERE device_code = 'DV-03') AS dv_03_avg_pressure_int,
        DENSE_RANK() OVER (PARTITION BY device_code ORDER BY COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) DESC) AS dv_04_count_dense_rank
    FROM deduped
    GROUP BY device_code, DATE_TRUNC('HOUR', reading_ts)
)
SELECT
    device_code,
    hour_window,
    first_reading_ts,
    dv_03_avg_pressure_int,
    dv_04_count_dense_rank
FROM daily
