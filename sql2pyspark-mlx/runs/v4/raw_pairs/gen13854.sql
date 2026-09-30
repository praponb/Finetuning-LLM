WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.unit_price > 130
        )
)
SELECT
    s.id,
    s.product_name,
    s.brand,
    s.price,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    (SELECT AVG(t2.stock_qty) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS avg_product_stock_qty
FROM kept s
ORDER BY cost DESC
