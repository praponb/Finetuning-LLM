SELECT oi.product_id, o.status, p.launch_date
FROM order_items oi
LEFT JOIN orders o ON o.id = oi.order_id
LEFT JOIN products p ON oi.product_id = p.id
