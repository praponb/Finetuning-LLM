SELECT carrier, ROUND(AVG(cost), 2) AS avg_cost FROM shipments WHERE delivered_date > '2025-08-01' GROUP BY carrier ORDER BY carrier, avg_cost LIMIT 1
