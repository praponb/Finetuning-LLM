WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        temp_c,
        status_bits,
        LEAD(humidity) OVER w_recent AS next_humidity,
        LAG(temp_c) OVER w_recent AS prev_temp_c,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM device_readings
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY temp_c, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(prev_temp_c) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_prev_temp_c,
        MIN(prev_temp_c) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_prev_temp_c,
        SUM(prev_temp_c) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_prev_temp_c
    FROM enriched
    WHERE temp_c BETWEEN 25 AND 37
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY running_min_prev_temp_c DESC, id) AS running_min_prev_temp_c_bucket,
        ROW_NUMBER() OVER (ORDER BY status_bits, id) AS event_seq
    FROM scored
),
scored_device_readings AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_sum_prev_temp_c) OVER (ORDER BY running_min_prev_temp_c, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_sum_prev_temp_c_avg_6,
        AVG(running_min_prev_temp_c_bucket) OVER (ORDER BY row_seq, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_min_prev_temp_c_bucket_avg_5_prior
    FROM with_windows
),
with_windows_device_readings AS (
    -- Running and moving aggregates
    SELECT
        id,
        next_humidity,
        prev_temp_c,
        running_min_prev_temp_c,
        running_sum_prev_temp_c,
        event_seq,
        running_sum_prev_temp_c_avg_6,
        running_min_prev_temp_c_bucket_avg_5_prior,
        FIRST_VALUE(running_sum_prev_temp_c) IGNORE NULLS OVER (ORDER BY running_sum_prev_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_sum_prev_temp_c,
        RANK() OVER w_hist AS prev_temp_c_rank,
        ROW_NUMBER() OVER (ORDER BY temp_c DESC, id) AS event_seq_2,
        AVG(running_sum_prev_temp_c_avg_6) OVER (ORDER BY prev_temp_c, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS running_sum_prev_temp_c_avg_6_avg_5,
        AVG(status_bits) OVER w_recent AS status_bits_avg_6_prior
    FROM scored_device_readings
    WINDOW w_hist AS (ORDER BY prev_temp_c DESC), w_recent AS (ORDER BY next_humidity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        prev_temp_c,
        running_min_prev_temp_c,
        running_sum_prev_temp_c,
        event_seq,
        running_sum_prev_temp_c_avg_6,
        running_min_prev_temp_c_bucket_avg_5_prior,
        prev_temp_c_rank,
        event_seq_2,
        running_sum_prev_temp_c_avg_6_avg_5,
        status_bits_avg_6_prior,
        ROUND(prev_temp_c * 0.9, 2) AS adjusted_prev_temp_c
    FROM with_windows_device_readings
    WHERE running_sum_prev_temp_c >= 90
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN status_bits_avg_6_prior >= 9 THEN 'HIGH' ELSE 'LOW' END AS status_bits_avg_6_prior_tier,
        COUNT(*) AS num_rows,
        APPROX_PERCENTILE(event_seq_2, 0.95) AS event_seq_2_p95,
        PERCENTILE_APPROX(prev_temp_c_rank, 0.9) AS prev_temp_c_rank_p90,
        COUNT(*) AS cnt
    FROM base
    GROUP BY CASE WHEN status_bits_avg_6_prior >= 9 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    status_bits_avg_6_prior_tier,
    num_rows,
    prev_temp_c_rank_p90,
    cnt
FROM totals
