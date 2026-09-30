WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(units * 1.1, 2) AS units_adjusted
    FROM store_sales
    WHERE product_id = 5
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY revenue ASC NULLS LAST) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) AS revenue_percent_rank,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC) AS revenue_bucket,
        LAST_VALUE(units_adjusted, TRUE) OVER (PARTITION BY product_id ORDER BY units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units_adjusted,
        SUM(units_adjusted) OVER (PARTITION BY store_id ORDER BY sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units_adjusted
    FROM deduped
    WHERE revenue BETWEEN 2200 AND 3700
),
metrics_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(revenue_bucket) OVER (PARTITION BY product_id ORDER BY revenue_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_revenue_bucket,
        SUM(revenue_bucket) OVER (PARTITION BY product_id ORDER BY revenue_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_revenue_bucket,
        AVG(revenue_bucket) OVER (PARTITION BY product_id ORDER BY revenue_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_revenue_bucket,
        SUM(running_units_adjusted) OVER (PARTITION BY product_id, store_id ORDER BY revenue_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_units_adjusted
    FROM metrics
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        revenue_percent_rank,
        revenue_bucket,
        running_max_revenue_bucket,
        running_sum_revenue_bucket,
        running_avg_revenue_bucket,
        cumulative_running_units_adjusted,
        ROUND(running_avg_revenue_bucket, 0) AS running_avg_revenue_bucket_rounded
    FROM metrics_store_sales
    WHERE product_id = 8
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        d.stock_qty,
        d.product_name
    FROM valid e
    INNER JOIN products d
        ON e.product_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    ROUND(AVG(cumulative_running_units_adjusted), 2) AS avg_cumulative_running_units_adjusted,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(running_max_revenue_bucket), 2) AS avg_running_max_revenue_bucket,
    APPROX_PERCENTILE(running_avg_revenue_bucket, 0.5) AS running_avg_revenue_bucket_p50,
    PERCENTILE_APPROX(cumulative_running_units_adjusted, ARRAY(0.25, 0.5, 0.75)) AS cumulative_running_units_adjusted_quartiles,
    COUNT(*) AS event_count
FROM with_dims
GROUP BY 1
