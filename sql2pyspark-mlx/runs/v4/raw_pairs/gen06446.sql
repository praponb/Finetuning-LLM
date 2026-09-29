SELECT
    oi.id,
    oi.product_id,
    p.launch_date,
    s.supplier_name,
    s.country
FROM order_items oi
RIGHT JOIN products p ON p.id = oi.product_id
INNER JOIN suppliers s ON s.id = p.supplier_id
ORDER BY p.launch_date ASC
