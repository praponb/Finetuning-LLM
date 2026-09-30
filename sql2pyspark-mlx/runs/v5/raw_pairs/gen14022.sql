SELECT MAX(cost) AS max_cost
FROM shipments
WHERE carrier IN ('UPS', 'Kerry')
ORDER BY max_cost DESC
