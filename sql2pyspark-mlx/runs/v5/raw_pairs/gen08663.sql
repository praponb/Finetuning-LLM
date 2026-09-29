SELECT status, MAX(delivered_date) AS last_delivered_date, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
WHERE MONTH(shipped_date) IN (4, 8, 12) AND carrier = 'Kerry'
GROUP BY status
ORDER BY last_delivered_date
