SELECT oi.id, oi.product_id, p.price
FROM order_items oi
JOIN products p ON p.id = oi.product_id
ORDER BY id
