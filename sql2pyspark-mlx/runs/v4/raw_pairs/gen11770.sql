SELECT carrier, AVG(cost) AS avg_cost FROM shipments GROUP BY carrier HAVING AVG(cost) < 93;
