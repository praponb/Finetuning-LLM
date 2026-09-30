SELECT id, status, cost, COUNT(*) OVER (PARTITION BY status) AS status_count, ROUND(cost / SUM(cost) OVER (PARTITION BY status), 3) AS cost_share FROM shipments ORDER BY id DESC, 4 DESC;
