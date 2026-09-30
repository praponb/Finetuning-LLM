SELECT o.channel, MAX(s.cost) AS max_cost, COUNT(s.cost) AS cost_count
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
GROUP BY o.channel
HAVING COUNT(s.cost) >= 3
ORDER BY cost_count DESC;
