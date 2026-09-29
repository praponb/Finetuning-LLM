SELECT delivered_date, shipped_date
FROM shipments
WHERE order_id IN (8, 9, 12)
UNION
SELECT delivered_date, shipped_date
FROM shipments
WHERE carrier NOT IN ('Kerry', 'FedEx')
