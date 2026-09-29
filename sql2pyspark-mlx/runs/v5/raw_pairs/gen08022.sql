WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.product_id = 9
        )
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.cost,
    s.stock_qty,
    s.tags,
    s.supplier_id,
    CASE WHEN brand RLIKE '^(Stark|Umbrella)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS brand_zone,
    (SELECT MAX(x.stock_qty) FROM products x WHERE x.supplier_id = s.supplier_id) AS max_product_stock_qty
FROM eligible s
