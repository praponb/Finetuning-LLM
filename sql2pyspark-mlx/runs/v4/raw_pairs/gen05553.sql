SELECT method, status
FROM payments
WHERE order_id IN (2, 17, 38)
UNION
SELECT method, status
FROM payments
WHERE order_id = 31
ORDER BY status
