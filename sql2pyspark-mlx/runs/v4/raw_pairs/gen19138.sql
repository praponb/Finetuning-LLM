WITH product_stats AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY product_id
)
SELECT p.supplier_id, p.launch_date, ps.quantity_range
FROM products p
LEFT JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.quantity_range > 79
ORDER BY quantity_range
