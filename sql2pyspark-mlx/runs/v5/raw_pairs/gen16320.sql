SELECT oi.id, oi.quantity
FROM order_items oi
WHERE oi.product_id IS NULL AND oi.order_id IN (SELECT id FROM orders WHERE order_date BETWEEN '2023-04-19' AND '2024-09-01')
