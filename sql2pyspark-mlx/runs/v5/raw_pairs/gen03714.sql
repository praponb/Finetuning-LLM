WITH by_dimension AS (
    -- Join a dimension and aggregate
    SELECT
        p.category,
        COUNT(*) AS num_rows,
        ROUND(SUM(s.revenue), 2) AS total_revenue,
        MIN(s.units) AS min_units
    FROM store_sales s
    LEFT JOIN products p ON s.product_id = p.id
    GROUP BY p.category
)
SELECT
    category,
    num_rows,
    min_units
FROM by_dimension;
