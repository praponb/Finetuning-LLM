SELECT SUM(cost) AS total_cost FROM shipments WHERE MONTH(shipped_date) IN (5, 7, 8) ORDER BY total_cost DESC
