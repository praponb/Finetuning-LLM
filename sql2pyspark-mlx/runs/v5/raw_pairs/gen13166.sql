WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        UPPER(city) AS city_upper,
        COALESCE(region, 'NONE') AS region_filled
    FROM stores
    WHERE opened_date > DATE '2023-07-26'
      AND city != 'Singapore'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sqft,
        city_upper,
        region_filled,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_sqft,
        COUNT(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft,
        SUM(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft
    FROM filtered
    WHERE sqft = 15529
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY sqft, id) AS sqft_bucket,
        RANK() OVER w_grp AS running_avg_sqft_rank
    FROM metrics
    WINDOW w_grp AS (ORDER BY running_avg_sqft DESC NULLS LAST)
),
metrics_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        region_filled,
        running_avg_sqft,
        running_count_sqft,
        sqft_bucket,
        ROW_NUMBER() OVER (ORDER BY running_count_sqft DESC, id) AS event_seq,
        SUM(running_count_sqft) OVER (ORDER BY running_count_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_count_sqft,
        LEAD(running_avg_sqft_rank, 2) OVER (ORDER BY sqft, id) AS next_running_avg_sqft_rank,
        ROW_NUMBER() OVER (ORDER BY running_sum_sqft DESC, id) AS event_seq_2
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN sqft_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS sqft_bucket_tier,
    PERCENTILE_APPROX(next_running_avg_sqft_rank, ARRAY(0.25, 0.5, 0.75)) AS next_running_avg_sqft_rank_quartiles,
    APPROX_PERCENTILE(sqft_bucket, 0.5) AS sqft_bucket_p50,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics_stores
GROUP BY CUBE (CASE WHEN sqft_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END);
