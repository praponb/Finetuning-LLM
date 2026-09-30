SELECT SUM(cost) AS total_cost
FROM shipments
WHERE shipped_date >= '2024-10-23'
ORDER BY total_cost DESC
