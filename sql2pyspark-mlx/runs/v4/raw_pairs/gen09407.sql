SELECT o.status, AVG(s.cost) AS avg_cost
FROM shipments s
JOIN orders o ON s.order_id = o.id
GROUP BY o.status
ORDER BY avg_cost, status
LIMIT 10;
