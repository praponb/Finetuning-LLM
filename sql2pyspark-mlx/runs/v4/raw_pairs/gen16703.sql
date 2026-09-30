SELECT status
FROM orders
WHERE MONTH(order_date) IN (6, 9, 12)
UNION ALL
SELECT status
FROM shipments
WHERE status = 'RETURNED'
