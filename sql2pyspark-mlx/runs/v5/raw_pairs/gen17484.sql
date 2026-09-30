SELECT status, SUM(cost) AS total_cost, MAX(cost) AS max_cost
FROM shipments
WHERE YEAR(shipped_date) >= 2023 OR carrier NOT IN ('DHL', 'Flash', 'UPS')
GROUP BY status
ORDER BY max_cost DESC;
