SELECT status, MIN(cost) AS min_cost, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
WHERE delivered_date > '2024-05-01'
GROUP BY status
HAVING min_cost < 89
ORDER BY status;
