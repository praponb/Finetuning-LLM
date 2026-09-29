SELECT status, MIN(delivered_date) AS min_delivered_date, MAX(cost) - MIN(cost) AS cost_range FROM shipments GROUP BY status ORDER BY 3 DESC, status LIMIT 10
