WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure
    FROM device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        SUM(pressure) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure,
        NTILE(5) OVER (ORDER BY humidity DESC, id) AS humidity_bucket,
        RANK() OVER w_grp AS humidity_rank,
        AVG(pressure) OVER (ORDER BY pressure, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS pressure_avg_2
    FROM deduped
    WINDOW w_grp AS (ORDER BY humidity)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        cumulative_pressure,
        humidity_bucket,
        AVG(temp_c) OVER w_running AS running_avg_temp_c,
        MAX(temp_c) OVER w_running AS running_max_temp_c,
        COUNT(temp_c) OVER w_running AS running_count_temp_c,
        MIN(temp_c) OVER w_running AS running_min_temp_c,
        NTILE(10) OVER w_running_2 AS humidity_bucket_2
    FROM with_windows
    WHERE humidity_bucket IS NULL
    WINDOW w_running AS (ORDER BY pressure_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (ORDER BY humidity DESC, id)
),
enriched_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        pressure,
        cumulative_pressure,
        humidity_bucket,
        running_avg_temp_c,
        running_max_temp_c,
        running_count_temp_c,
        running_min_temp_c,
        AVG(running_min_temp_c) OVER (ORDER BY humidity_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_min_temp_c,
        MAX(running_min_temp_c) OVER (ORDER BY humidity_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_min_temp_c,
        COUNT(running_min_temp_c) OVER (ORDER BY humidity_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_min_temp_c,
        SUM(running_min_temp_c) OVER (ORDER BY humidity_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_min_temp_c,
        AVG(running_avg_temp_c) OVER (ORDER BY pressure, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS running_avg_temp_c_avg_2_prior
    FROM enriched
),
with_windows_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(running_max_temp_c) OVER (ORDER BY running_min_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_max_temp_c,
        COUNT(running_max_temp_c) OVER (ORDER BY running_min_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_max_temp_c,
        SUM(running_max_temp_c) OVER (ORDER BY running_min_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_max_temp_c,
        AVG(running_max_temp_c) OVER (ORDER BY running_max_running_min_temp_c, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_max_temp_c_avg_3
    FROM enriched_device_readings
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        humidity_bucket,
        running_avg_temp_c,
        running_max_temp_c,
        running_min_temp_c,
        running_max_running_min_temp_c,
        running_count_running_min_temp_c,
        running_avg_running_max_temp_c,
        CUME_DIST() OVER (ORDER BY running_avg_running_max_temp_c DESC NULLS LAST) AS running_avg_running_max_temp_c_cume_dist,
        CUME_DIST() OVER (ORDER BY running_sum_running_min_temp_c) AS running_sum_running_min_temp_c_cume_dist,
        LEAD(running_avg_temp_c_avg_2_prior, 1, 0) OVER (ORDER BY running_count_temp_c, id) AS next_running_avg_temp_c_avg_2_prior,
        SUM(running_avg_running_max_temp_c) OVER (ORDER BY running_min_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_avg_running_max_temp_c
    FROM with_windows_device_readings
)
-- Window metrics over several different partitions
SELECT
    id,
    humidity_bucket,
    running_avg_temp_c,
    running_max_temp_c,
    running_min_temp_c,
    running_max_running_min_temp_c,
    running_count_running_min_temp_c,
    running_avg_running_max_temp_c,
    running_sum_running_min_temp_c_cume_dist,
    next_running_avg_temp_c_avg_2_prior,
    LAG(humidity_bucket) OVER (ORDER BY humidity_bucket, id) AS prev_humidity_bucket,
    LAG(running_max_running_min_temp_c) OVER (ORDER BY humidity_bucket, id) AS prev_running_max_running_min_temp_c,
    LEAD(running_max_running_min_temp_c) OVER (ORDER BY humidity_bucket, id) AS next_running_max_running_min_temp_c,
    ROW_NUMBER() OVER (ORDER BY humidity_bucket, id) AS seq_no
FROM metrics
