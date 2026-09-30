SELECT carrier, MAX(cost) AS max_cost, COUNT(cost) AS cost_count FROM shipments GROUP BY carrier
