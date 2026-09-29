SELECT * FROM (SELECT carrier, SUM(cost) total_cost, ROUND(STDDEV(cost), 2) stddev_cost FROM shipments GROUP BY carrier) s WHERE total_cost >= 260
