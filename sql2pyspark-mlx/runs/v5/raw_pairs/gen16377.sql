SELECT id, delivered_date, shipped_date
FROM shipments
WHERE status = 'LOST'
UNION ALL
SELECT id, delivered_date, shipped_date
FROM shipments
WHERE shipped_date < '2024-11-01';
