SELECT order_id, COUNT(DISTINCT amount) AS unique_orders
FROM payments
GROUP BY order_id;
