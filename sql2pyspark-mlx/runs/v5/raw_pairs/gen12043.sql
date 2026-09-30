SELECT carrier, MAX(cost) - MIN(cost) AS cost_range FROM shipments GROUP BY carrier HAVING MAX(cost) - MIN(cost) < 24 ORDER BY carrier, cost_range DESC LIMIT 50;
