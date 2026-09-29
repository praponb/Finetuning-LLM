WITH with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        b.cost,
        b.price
    FROM store_sales a
    LEFT JOIN products b
        ON a.product_id = b.id
)
-- Multi-level aggregation
SELECT
    store_id,
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(cost), 2) AS avg_cost,
    SUM(price) AS total_price,
    APPROX_PERCENTILE(price, 0.9) AS price_p90
FROM with_dims
GROUP BY GROUPING SETS ((store_id, DATE_TRUNC('MONTH', sale_date)), (store_id), ())
ORDER BY activity_month
