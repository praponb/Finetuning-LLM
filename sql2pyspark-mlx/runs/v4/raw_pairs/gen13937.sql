SELECT * FROM (SELECT status, COUNT(cost) AS cost_count FROM shipments GROUP BY status) AS s WHERE cost_count >= 5;
