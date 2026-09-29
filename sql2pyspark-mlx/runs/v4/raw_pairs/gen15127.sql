SELECT id, status, cost, ROUND(cost / SUM(cost) OVER (PARTITION BY status), 3) AS cost_share FROM shipments WHERE shipped_date BETWEEN '2025-05-13' AND '2025-06-01'
