WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        DATE_TRUNC('YEAR', reading_ts) AS reading_ts_period,
        temp_c * humidity AS temp_c_times_humidity
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 04:00:00'
      AND pressure > 1013
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        firmware,
        reading_ts_period,
        LEAD(pressure, 2, 0) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS next_pressure,
        AVG(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS status_bits_avg_6_prior,
        SUM(pressure) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS pressure_sum_30m
    FROM base
    WHERE temp_c_times_humidity BETWEEN 36 AND 66
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        firmware,
        reading_ts_period,
        next_pressure,
        pressure_sum_30m,
        SUM(pressure_sum_30m) OVER (PARTITION BY device_code ORDER BY reading_ts_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure_sum_30m,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts_period DESC, id) AS row_seq,
        CUME_DIST() OVER (ORDER BY temp_c) AS temp_c_cume_dist
    FROM with_windows
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(next_pressure) OVER (PARTITION BY device_code ORDER BY temp_c_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_next_pressure,
        AVG(next_pressure) OVER (PARTITION BY device_code ORDER BY temp_c_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_next_pressure,
        COUNT(next_pressure) OVER (PARTITION BY device_code ORDER BY temp_c_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_next_pressure,
        SUM(next_pressure) OVER (PARTITION BY device_code ORDER BY temp_c_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_next_pressure
    FROM enriched
),
scored_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        temp_c,
        reading_ts_period,
        pressure_sum_30m,
        last7_count_next_pressure,
        AVG(temp_c_cume_dist) OVER w_recent AS temp_c_cume_dist_avg_2_prior,
        CORR(temp_c, pressure) OVER (PARTITION BY device_code) AS corr_temp_c_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY row_seq, id) AS event_seq,
        LAG(pressure_sum_30m, 2) OVER w_hist AS prev_pressure_sum_30m
    FROM scored
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY cumulative_pressure_sum_30m, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_hist AS (PARTITION BY device_code ORDER BY temp_c_cume_dist, id)
),
scored_device_readings_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        pressure_sum_30m,
        last7_count_next_pressure,
        temp_c_cume_dist_avg_2_prior,
        MIN(prev_pressure_sum_30m) OVER (PARTITION BY device_code ORDER BY last7_count_next_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_prev_pressure_sum_30m,
        MAX(prev_pressure_sum_30m) OVER (PARTITION BY device_code ORDER BY last7_count_next_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_pressure_sum_30m,
        AVG(prev_pressure_sum_30m) OVER (PARTITION BY device_code ORDER BY last7_count_next_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_prev_pressure_sum_30m
    FROM scored_device_readings
),
with_windows_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(last7_count_next_pressure) OVER w_running AS running_max_last7_count_next_pressure,
        AVG(last7_count_next_pressure) OVER w_running AS running_avg_last7_count_next_pressure,
        MIN(last7_count_next_pressure) OVER w_running AS running_min_last7_count_next_pressure
    FROM scored_device_readings_2
    WINDOW w_running AS (ORDER BY running_min_prev_pressure_sum_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_max_last7_count_next_pressure >= 14.8 THEN 'LARGE' ELSE 'SMALL' END AS running_max_last7_count_next_pressure_tier,
    APPROX_PERCENTILE(running_min_last7_count_next_pressure, 0.5) AS running_min_last7_count_next_pressure_p50,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(last7_count_next_pressure, 0.9) AS last7_count_next_pressure_p90,
    SUM(running_min_last7_count_next_pressure) AS total_running_min_last7_count_next_pressure,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(running_avg_prev_pressure_sum_30m, ARRAY(0.25, 0.5, 0.75)) AS running_avg_prev_pressure_sum_30m_quartiles
FROM with_windows_device_readings
GROUP BY running_max_last7_count_next_pressure_tier
ORDER BY row_count DESC NULLS LAST;
