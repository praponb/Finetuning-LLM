WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.sale_date,
        a.revenue,
        b.sqft
    FROM store_sales a
    LEFT JOIN stores b
        ON a.store_id = b.id
    WHERE b.opened_date < '2023-10-14'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
        AVG(revenue) OVER w_running AS revenue_avg_2_prior,
        LAST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue
    FROM with_dims
    WINDOW w_running AS (ORDER BY revenue, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        revenue,
        sqft,
        revenue_avg_2_prior,
        last_known_revenue,
        ROUND(revenue_avg_2_prior / SUM(revenue_avg_2_prior) OVER (PARTITION BY store_id), 4) AS revenue_avg_2_prior_share,
        SUM(sqft) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        SUM(revenue) OVER w_running AS cumulative_revenue
    FROM with_windows
    WINDOW w_running AS (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(revenue_avg_2_prior_share) AS revenue_avg_2_prior_share_whole
    FROM enriched
    WHERE revenue_avg_2_prior > 1938
)
-- Join a dimension and aggregate
SELECT
    p.category,
    p.brand,
    COUNT(*) AS rows_total,
    SUM(e.cumulative_revenue) AS total_cumulative_revenue,
    ROUND(SUM(e.revenue_avg_2_prior), 2) AS total_revenue_avg_2_prior
FROM cleaned e
LEFT JOIN products p ON e.product_id = p.id
GROUP BY p.category, p.brand;
