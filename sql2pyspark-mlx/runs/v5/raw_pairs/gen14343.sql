SELECT status, cost_count, cnt FROM (SELECT status, COUNT(cost) AS cost_count, COUNT(*) AS cnt FROM shipments GROUP BY status) AS s WHERE cnt > 6
