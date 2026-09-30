SELECT order_id, cost_count FROM (SELECT order_id, COUNT(cost) AS cost_count FROM shipments GROUP BY order_id) AS s WHERE cost_count >= 3 ORDER BY order_id DESC, 2 DESC;
