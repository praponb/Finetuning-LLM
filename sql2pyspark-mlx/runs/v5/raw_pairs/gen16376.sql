SELECT
    oi.id,
    oi.product_id,
    oi.quantity,
    o.status,
    p.product_name
FROM order_items oi
RIGHT JOIN orders o ON oi.order_id = o.id
RIGHT JOIN products p ON oi.product_id = p.id
WHERE oi.product_id IS NOT NULL
ORDER BY oi.quantity DESC
