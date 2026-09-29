SELECT carrier, SUM(cost) AS total_cost, ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
GROUP BY carrier
ORDER BY stddev_cost DESC, carrier
LIMIT 5;
