WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(pressure AS INT) pressure_int
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(pressure) OVER w_ordered pressure_avg_2_prior,
        CORR(status_bits, pressure) OVER w_grp corr_status_bits_pressure,
        AVG(pressure) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) pressure_avg_30m
    FROM base
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_grp AS (PARTITION BY device_code)
),
with_windows_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        firmware,
        pressure_avg_2_prior,
        pressure_avg_30m,
        SUM(pressure_avg_30m) OVER w_grp cumulative_pressure_avg_30m,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_pressure
    FROM with_windows
    WHERE corr_status_bits_pressure >= 0.2
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        pressure_avg_2_prior,
        pressure_avg_30m,
        cumulative_pressure_avg_30m,
        running_pressure,
        CASE WHEN temp_c >= 39.6 THEN 'High' WHEN temp_c >= 25.3 THEN 'Medium' ELSE 'Low' END temp_c_level,
        ROUND(pressure * 1.07, 2) adjusted_pressure
    FROM with_windows_device_readings
    WHERE humidity >= 37
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM cleaned
    INTERSECT
    SELECT id
    FROM device_readings
    WHERE pressure >= 1017
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        temp_c,
        humidity,
        pressure_avg_2_prior,
        pressure_avg_30m,
        cumulative_pressure_avg_30m,
        running_pressure,
        temp_c_level,
        adjusted_pressure,
        DENSE_RANK() OVER (ORDER BY temp_c NULLS LAST) temp_c_dense_rank,
        PERCENT_RANK() OVER (ORDER BY temp_c) temp_c_percent_rank,
        CUME_DIST() OVER (ORDER BY pressure DESC NULLS LAST) pressure_cume_dist
    FROM kept_rows
),
enriched_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered temp_c_percent_rank_2,
        DENSE_RANK() OVER (ORDER BY humidity DESC) humidity_dense_rank
    FROM enriched
    WINDOW w_ordered AS (ORDER BY temp_c DESC NULLS LAST)
)
SELECT
    temp_c,
    pressure_avg_2_prior,
    pressure_avg_30m,
    cumulative_pressure_avg_30m,
    running_pressure,
    temp_c_level,
    adjusted_pressure,
    temp_c_dense_rank,
    temp_c_percent_rank
FROM enriched_device_readings
ORDER BY temp_c_percent_rank DESC
