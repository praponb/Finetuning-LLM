WITH joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.launch_date,
        d.product_name
    FROM store_sales s
    LEFT JOIN products d
        ON s.product_id = d.id
),
with_dims AS (
    -- Attach products attributes
    SELECT s.*, p.price
    FROM joined s
    LEFT JOIN products p
        ON s.product_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share,
        NTILE(5) OVER (ORDER BY revenue, id) AS revenue_bucket,
        NTILE(4) OVER (ORDER BY revenue, id) AS revenue_bucket_2,
        RANK() OVER (PARTITION BY product_id ORDER BY units) AS units_rank,
        LAST_VALUE(revenue, TRUE) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue
    FROM with_dims
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_known_revenue DESC NULLS FIRST, id) = 1
)
-- Multi-level aggregation
SELECT
    product_id,
    PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    APPROX_PERCENTILE(revenue_bucket_2, ARRAY(0.25, 0.5, 0.75)) AS revenue_bucket_2_quartiles
FROM ranked
GROUP BY product_id
ORDER BY product_id DESC;
