SELECT carrier, ROUND(STDDEV(cost), 2) AS stddev_cost FROM shipments GROUP BY carrier ORDER BY stddev_cost, carrier LIMIT 50;
