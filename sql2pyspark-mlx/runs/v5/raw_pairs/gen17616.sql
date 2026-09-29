SELECT status, MIN(delivered_date) AS first_delivered_date, COUNT(cost) AS cost_count FROM shipments WHERE status IN ('IN_TRANSIT', 'LOST') GROUP BY status ORDER BY 1, cost_count DESC;
