SELECT order_id
FROM payments
WHERE order_id = 40
UNION ALL
SELECT order_id
FROM payments
WHERE order_id IN (9, 24)
