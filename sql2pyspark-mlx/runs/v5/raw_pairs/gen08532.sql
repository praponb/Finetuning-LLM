SELECT o.store_id, COUNT(DISTINCT oi.quantity) AS unique_orders
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
GROUP BY o.store_id
ORDER BY 2, store_id
LIMIT 10;
