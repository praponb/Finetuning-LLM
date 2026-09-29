SELECT o.id, o.channel, t.avg_cost FROM orders o INNER JOIN (SELECT order_id, ROUND(AVG(cost), 2) AS avg_cost FROM shipments GROUP BY order_id) t ON o.id = t.order_id;
