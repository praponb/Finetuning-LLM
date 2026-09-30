SELECT id, shipped_date, status
FROM shipments
WHERE shipped_date > (SELECT MAX(shipped_date) FROM shipments WHERE status = 'DELIVERED')
ORDER BY shipped_date DESC;
