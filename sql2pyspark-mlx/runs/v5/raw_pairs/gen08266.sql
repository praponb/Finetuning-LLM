SELECT o.amount, o.order_date, t.avg_cost FROM orders o LEFT JOIN (SELECT order_id, ROUND(AVG(cost), 2) AS avg_cost FROM shipments WHERE order_id IS NULL GROUP BY order_id) t ON o.id = t.order_id
