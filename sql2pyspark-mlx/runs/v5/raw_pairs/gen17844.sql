WITH by_dimension AS (
    -- Join a dimension and aggregate
    SELECT
        p.brand,
        p.category,
        COUNT(*) AS cnt,
        MIN(s.quantity) AS min_quantity,
        GROUPING_ID() AS grouping_level
    FROM inventory s
    JOIN products p ON s.product_id = p.id
    GROUP BY GROUPING SETS ((p.brand, p.category), (p.brand))
)
SELECT
    brand,
    category,
    grouping_level
FROM by_dimension
ORDER BY category DESC NULLS FIRST;
