SELECT id, shipped_date, status
FROM shipments
WHERE shipped_date = (SELECT MIN(shipped_date) FROM shipments WHERE delivered_date < '2024-05-01')
ORDER BY status, 2
