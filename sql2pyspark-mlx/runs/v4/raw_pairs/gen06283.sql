WITH filtered AS (
    SELECT
        id,
        store_name,
        region,
        sqft,
        UPPER(store_name) AS store_name_upper
    FROM stores
    WHERE opened_date > DATE '2023-09-26'
),
eligible AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.store_id = 1
        )
),
metrics AS (
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket,
        LEAD(sqft, 1) OVER (ORDER BY sqft, id) AS next_sqft,
        LAG(sqft, 2, 0) OVER (ORDER BY sqft, id) AS prev_sqft
    FROM eligible
),
metrics_stores AS (
    SELECT
        id,
        store_name,
        region,
        sqft,
        store_name_upper,
        cumulative_sqft,
        sqft_bucket,
        next_sqft,
        prev_sqft,
        SUM(sqft_bucket) OVER (PARTITION BY region ORDER BY next_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft_bucket,
        LEAD(sqft_bucket, 2) OVER (PARTITION BY region ORDER BY cumulative_sqft, id) AS next_sqft_bucket,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS seq_no,
        PERCENT_RANK() OVER (ORDER BY sqft NULLS FIRST) AS sqft_percent_rank
    FROM metrics
),
metrics_stores_2 AS (
    SELECT
        *,
        SUM(cumulative_sqft) OVER (PARTITION BY region) AS region_sum_cumulative_sqft,
        AVG(next_sqft) OVER w_running AS next_sqft_avg_6_prior,
        ROUND(next_sqft / SUM(next_sqft) OVER (PARTITION BY region), 4) AS next_sqft_share
    FROM metrics_stores
    WINDOW w_running AS (PARTITION BY region ORDER BY cumulative_sqft_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
with_windows AS (
    SELECT
        *,
        LAG(seq_no, 1) OVER (PARTITION BY region ORDER BY prev_sqft, id) AS prev_seq_no,
        SUM(next_sqft) OVER (PARTITION BY region ORDER BY next_sqft_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_sqft
    FROM metrics_stores_2
),
cleaned AS (
    SELECT
        *,
        CAST(next_sqft_bucket AS DOUBLE) AS next_sqft_bucket_str,
        ROUND(prev_seq_no / cumulative_sqft, 2) AS prev_seq_no_per_cumulative_sqft,
        COALESCE(region, 'NONE') AS resolved_region
    FROM with_windows
    WHERE region IS NOT NULL
)
SELECT *
FROM (
    SELECT
        id,
        region,
        next_sqft_bucket_str,
        sqft_percent_rank,
        next_sqft_share
    FROM cleaned
) src
UNPIVOT (
    reading FOR kpi IN (next_sqft_bucket_str, sqft_percent_rank, next_sqft_share)
);
