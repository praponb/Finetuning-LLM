SELECT carrier, cost_range FROM (SELECT carrier, MAX(cost) - MIN(cost) AS cost_range FROM shipments GROUP BY carrier) AS x WHERE cost_range < 37 ORDER BY 1 DESC, cost_range LIMIT 3;
