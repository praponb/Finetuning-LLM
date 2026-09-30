SELECT orders.status, SUM(order_items.quantity) AS total_quantity, AVG(order_items.unit_price) AS avg_unit_price
FROM order_items
JOIN orders ON order_items.order_id = orders.id
WHERE orders.order_date >= '2024-07-01'
GROUP BY orders.status
ORDER BY status ASC
LIMIT 3;
