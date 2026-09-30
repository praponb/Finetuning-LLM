WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.quantity <= 3
        )
)
SELECT
    s.id,
    s.product_name,
    s.price,
    s.stock_qty,
    s.launch_date,
    s.supplier_id,
    CASE WHEN product_name RLIKE '^(Laptop Pro 14|Running Shoes)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS product_name_zone,
    (SELECT AVG(p2.cost) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS avg_product_cost
FROM eligible s
