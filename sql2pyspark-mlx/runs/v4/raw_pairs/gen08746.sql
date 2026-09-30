SELECT o.status, COUNT(DISTINCT s.cost) unique_statuss, SUM(s.cost) total_cost
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
GROUP BY o.status;
