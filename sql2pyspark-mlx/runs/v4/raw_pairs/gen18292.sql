SELECT id, delivered_date
FROM shipments
WHERE order_id IS NOT NULL
UNION
SELECT id, delivered_date
FROM shipments
WHERE carrier = 'Flash'
ORDER BY id;
