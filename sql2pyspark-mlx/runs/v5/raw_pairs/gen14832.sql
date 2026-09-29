SELECT carrier, MIN(cost) AS min_cost, AVG(cost) AS avg_cost FROM shipments WHERE shipped_date < '2023-05-22' AND status IN ('LOST', 'DELIVERED', 'RETURNED') GROUP BY carrier ORDER BY avg_cost DESC;
