SELECT id, delivered_date
FROM shipments
WHERE status <> 'RETURNED'
