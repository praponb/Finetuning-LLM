SELECT o.status, SUM(CASE WHEN s.status = 'LOST' THEN s.cost ELSE 0 END) AS lost_cost, MAX(s.cost) - MIN(s.cost) AS cost_range
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
GROUP BY o.status
ORDER BY lost_cost DESC, status
LIMIT 1;
