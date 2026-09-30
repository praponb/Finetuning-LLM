WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.product_id IN (1, 9, 10)
        )
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.price,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    CASE WHEN category RLIKE '^(Books|Home)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS category_zone,
    (SELECT MIN(p2.cost) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS min_product_cost
FROM not_flagged s
ORDER BY category NULLS FIRST;
