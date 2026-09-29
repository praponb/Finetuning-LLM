SELECT status, carrier, ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
GROUP BY status, carrier
ORDER BY 2;
