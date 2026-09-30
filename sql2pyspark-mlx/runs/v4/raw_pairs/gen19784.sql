SELECT order_id
FROM shipments
WHERE delivered_date < '2023-03-01'
UNION
SELECT order_id
FROM shipments
WHERE status = 'IN_TRANSIT'
