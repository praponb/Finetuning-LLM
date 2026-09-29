SELECT o.store_id, ROUND(STDDEV(s.cost), 2) AS stddev_cost
FROM shipments s
RIGHT JOIN orders o ON s.order_id = o.id
GROUP BY o.store_id
