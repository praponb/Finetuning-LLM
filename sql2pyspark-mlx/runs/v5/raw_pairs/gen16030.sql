WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        SUM(temp_c) OVER (PARTITION BY device_code) AS device_code_sum_temp_c,
        MAX(status_bits) OVER w_grp AS status_bits_max_30m,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pressure
    FROM device_readings
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        device_code_sum_temp_c,
        cumulative_pressure,
        MAX(device_code_sum_temp_c) OVER w_hist AS running_max_device_code_sum_temp_c,
        MIN(device_code_sum_temp_c) OVER w_hist AS running_min_device_code_sum_temp_c,
        SUM(device_code_sum_temp_c) OVER w_hist AS running_sum_device_code_sum_temp_c
    FROM metrics
    WHERE status_bits_max_30m = 5
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY status_bits_max_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(device_code_sum_temp_c, running_sum_device_code_sum_temp_c) AS least_device_code_sum_temp_c_running_sum_device_code_sum_temp_c
    FROM with_windows
    WHERE device_code_sum_temp_c != 114
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(device_code_sum_temp_c) OVER (PARTITION BY device_code) AS device_code_avg_device_code_sum_temp_c,
        SUM(running_min_device_code_sum_temp_c) OVER (PARTITION BY device_code ORDER BY running_sum_device_code_sum_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_min_device_code_sum_temp_c,
        LEAD(device_code_sum_temp_c, 2) OVER (PARTITION BY device_code ORDER BY running_max_device_code_sum_temp_c, id) AS next_device_code_sum_temp_c,
        SUM(cumulative_pressure) OVER w_hist AS cumulative_cumulative_pressure
    FROM base
    WINDOW w_hist AS (ORDER BY cumulative_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(running_max_device_code_sum_temp_c) OVER (PARTITION BY device_code ORDER BY cumulative_cumulative_pressure, id) AS next_running_max_device_code_sum_temp_c,
        LAG(running_max_device_code_sum_temp_c) OVER (PARTITION BY device_code ORDER BY cumulative_cumulative_pressure, id) AS prev_running_max_device_code_sum_temp_c,
        LAG(running_min_device_code_sum_temp_c) OVER (PARTITION BY device_code ORDER BY cumulative_cumulative_pressure, id) AS prev_running_min_device_code_sum_temp_c,
        LEAD(running_min_device_code_sum_temp_c) OVER (PARTITION BY device_code ORDER BY cumulative_cumulative_pressure, id) AS next_running_min_device_code_sum_temp_c,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY cumulative_cumulative_pressure, id) AS event_seq,
        MAX(next_device_code_sum_temp_c) OVER (PARTITION BY device_code) AS device_code_max_next_device_code_sum_temp_c
    FROM enriched
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_running_min_device_code_sum_temp_c >= 149 THEN 'LARGE' ELSE 'SMALL' END AS next_running_min_device_code_sum_temp_c_tier,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    COUNT(*) AS event_count,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT(*) AS event_count_2
FROM scored
GROUP BY CASE WHEN next_running_min_device_code_sum_temp_c >= 149 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY num_device_codes DESC;
