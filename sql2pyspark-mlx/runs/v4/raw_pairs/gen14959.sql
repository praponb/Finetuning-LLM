SELECT MAX(shipped_date) AS last_shipped_date
FROM shipments
WHERE delivered_date < '2025-01-01'
ORDER BY last_shipped_date;
