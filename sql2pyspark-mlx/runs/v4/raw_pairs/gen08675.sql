SELECT status, MIN(shipped_date) AS min_shipped_date, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
GROUP BY status
ORDER BY min_shipped_date;
