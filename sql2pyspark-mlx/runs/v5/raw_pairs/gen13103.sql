SELECT o.id, o.amount, o.channel, t.min_cost FROM orders o LEFT OUTER JOIN (SELECT order_id, MIN(cost) AS min_cost FROM shipments GROUP BY order_id) t ON o.id = t.order_id ORDER BY amount DESC
