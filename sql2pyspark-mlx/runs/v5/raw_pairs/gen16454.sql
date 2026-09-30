WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        region,
        sqft,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_sqft,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft,
        MIN(sqft) OVER (PARTITION BY city) city_min_sqft
    FROM stores
),
metrics_stores AS (
    -- Running and moving aggregates
    SELECT
        id,
        region,
        sqft,
        first_known_sqft,
        running_sqft,
        LEAD(first_known_sqft) OVER (PARTITION BY region ORDER BY running_sqft, id) next_first_known_sqft,
        LAG(running_sqft) OVER (PARTITION BY region ORDER BY running_sqft, id) prev_running_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY running_sqft, id) event_seq
    FROM metrics
    WHERE sqft <= 5097
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        sqft,
        first_known_sqft,
        running_sqft,
        next_first_known_sqft,
        prev_running_sqft,
        event_seq,
        AVG(running_sqft) OVER w_hist region_avg_running_sqft,
        MIN(prev_running_sqft) OVER w_hist region_min_prev_running_sqft,
        SUM(sqft) OVER (PARTITION BY region ORDER BY next_first_known_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_sqft
    FROM metrics_stores
    WINDOW w_hist AS (PARTITION BY region)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(region_avg_running_sqft) OVER (ORDER BY first_known_sqft, id) next_region_avg_running_sqft,
        LEAD(sqft) OVER (ORDER BY first_known_sqft, id) next_sqft,
        LAG(sqft) OVER (ORDER BY first_known_sqft, id) prev_sqft,
        ROW_NUMBER() OVER (ORDER BY first_known_sqft, id) event_seq_2,
        LEAD(region_min_prev_running_sqft, 2) OVER (ORDER BY region_avg_running_sqft, id) next_region_min_prev_running_sqft
    FROM enriched
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(region_avg_running_sqft, 2) OVER (ORDER BY sqft, id) next_region_avg_running_sqft_2,
        CUME_DIST() OVER (ORDER BY event_seq_2 DESC NULLS LAST) event_seq_2_cume_dist,
        LAST_VALUE(next_region_min_prev_running_sqft, TRUE) OVER (ORDER BY running_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_next_region_min_prev_running_sqft,
        ROW_NUMBER() OVER (ORDER BY prev_running_sqft DESC, id) seq_no
    FROM scored
)
-- Multi-level aggregation
SELECT
    CASE WHEN last_known_next_region_min_prev_running_sqft >= 74209 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_next_region_min_prev_running_sqft_tier,
    CASE WHEN event_seq_2_cume_dist >= 0.81 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_2_cume_dist_tier,
    seq_no,
    ROUND(AVG(running_sqft), 2) AS avg_running_sqft,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    MAX(next_region_avg_running_sqft) AS max_next_region_avg_running_sqft
FROM with_windows
GROUP BY CUBE (CASE WHEN last_known_next_region_min_prev_running_sqft >= 74209 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN event_seq_2_cume_dist >= 0.81 THEN 'PREMIUM' ELSE 'STANDARD' END, seq_no)
ORDER BY avg_running_sqft NULLS LAST;
