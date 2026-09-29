SELECT id, delivered_date, shipped_date
FROM shipments
WHERE delivered_date = (SELECT MIN(delivered_date) FROM shipments)
ORDER BY shipped_date DESC, id
LIMIT 3
