SELECT oi.id, oi.order_id
FROM order_items oi
WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE discount IS NULL)
