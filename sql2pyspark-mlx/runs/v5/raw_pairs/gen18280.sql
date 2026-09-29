WITH dim_totals AS (
    SELECT
        p.brand,
        p.category,
        COUNT(*) AS num_rows,
        MAX(e.quantity) AS max_quantity
    FROM inventory e
    INNER JOIN products p ON e.product_id = p.id
    GROUP BY p.brand, p.category
)
SELECT
    brand,
    category,
    num_rows,
    max_quantity
FROM dim_totals
ORDER BY brand DESC NULLS FIRST, num_rows DESC;
