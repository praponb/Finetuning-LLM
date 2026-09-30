WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue * units AS revenue_per_unit
    FROM store_sales
    WHERE product_id IN (1, 4, 6)
)
-- Join a dimension and aggregate
SELECT
    p.category,
    COUNT(*) AS rows_total,
    AVG(e.units) AS avg_units,
    MIN(e.revenue) AS min_revenue,
    GROUPING_ID() AS grouping_level
FROM filtered e
LEFT JOIN products p ON e.product_id = p.id
GROUP BY ROLLUP (p.category)
ORDER BY avg_units NULLS LAST;
