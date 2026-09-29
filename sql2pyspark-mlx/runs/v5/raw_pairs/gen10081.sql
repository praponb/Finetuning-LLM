SELECT carrier, shipped_date
FROM shipments
WHERE delivered_date >= '2024-01-25'
UNION
SELECT carrier, shipped_date
FROM shipments
WHERE order_id IS NOT NULL
