SELECT status, ROUND(AVG(cost), 2) AS avg_cost, COUNT(DISTINCT cost) AS unique_orders FROM shipments GROUP BY 1 HAVING COUNT(DISTINCT cost) >= 4 ORDER BY unique_orders DESC, status DESC
