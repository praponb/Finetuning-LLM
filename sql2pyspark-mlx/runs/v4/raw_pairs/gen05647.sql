WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        ROUND(pressure / temp_c, 2) AS pressure_per_temp_c
    FROM device_readings
    WHERE temp_c BETWEEN 25 AND 34
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(pressure_per_temp_c, 1) OVER (ORDER BY reading_ts, id) AS next_pressure_per_temp_c,
        ROW_NUMBER() OVER (ORDER BY pressure_per_temp_c DESC, id) AS seq_no,
        DENSE_RANK() OVER (ORDER BY humidity DESC) AS humidity_dense_rank
    FROM cleaned
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, humidity_dense_rank + pressure_per_temp_c AS humidity_dense_rank_plus_pressure_per_temp_c
    FROM metrics
    WHERE pressure_per_temp_c BETWEEN 23 AND 46
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(humidity_dense_rank) OVER (ORDER BY next_pressure_per_temp_c, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS humidity_dense_rank_avg_5_prior,
        LAST_VALUE(pressure_per_temp_c, TRUE) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure_per_temp_c
    FROM valid
),
metrics_device_readings AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        firmware,
        pressure_per_temp_c,
        next_pressure_per_temp_c,
        seq_no,
        humidity_dense_rank,
        humidity_dense_rank_plus_pressure_per_temp_c,
        humidity_dense_rank_avg_5_prior,
        last_known_pressure_per_temp_c,
        CUME_DIST() OVER (ORDER BY humidity_dense_rank) AS humidity_dense_rank_cume_dist,
        SUM(temp_c) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS temp_c_sum_30m,
        NTILE(10) OVER w_recent AS last_known_pressure_per_temp_c_bucket,
        SUM(pressure_per_temp_c) OVER (ORDER BY last_known_pressure_per_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure_per_temp_c,
        AVG(next_pressure_per_temp_c) OVER w_hist AS next_pressure_per_temp_c_avg_1h
    FROM scored
    WINDOW w_recent AS (ORDER BY last_known_pressure_per_temp_c DESC, id), w_hist AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    CASE WHEN temp_c >= 39.6 THEN 'LARGE' ELSE 'SMALL' END AS temp_c_tier,
    SUM(last_known_pressure_per_temp_c_bucket) AS total_last_known_pressure_per_temp_c_bucket,
    PERCENTILE_APPROX(temp_c, 0.5) AS temp_c_p50,
    PERCENTILE_APPROX(last_known_pressure_per_temp_c, 0.9) AS last_known_pressure_per_temp_c_p90,
    COUNT(*) AS event_count,
    MAX(reading_ts) AS last_reading_ts
FROM metrics_device_readings
GROUP BY DATE_TRUNC('HOUR', reading_ts), CASE WHEN temp_c >= 39.6 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY total_last_known_pressure_per_temp_c_bucket;
