SELECT status, COUNT(DISTINCT cost) AS unique_carriers, MIN(cost) AS min_cost FROM shipments GROUP BY 1 ORDER BY min_cost DESC, status DESC;
