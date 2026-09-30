WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(revenue, 0) revenue_filled
    FROM store_sales
    WHERE sale_date > DATE '2023-05-21'
)
-- Join a dimension and aggregate
SELECT
    p.category,
    COUNT(*) AS rows_total,
    AVG(a.revenue_filled) AS avg_revenue_filled
FROM cleaned a
INNER JOIN products p ON a.product_id = p.id
GROUP BY ROLLUP (p.category)
