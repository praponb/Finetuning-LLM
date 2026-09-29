SELECT status, order_id
FROM payments
WHERE HOUR(paid_at) BETWEEN 7 AND 15
UNION ALL
SELECT status, order_id
FROM payments
WHERE order_id IS NULL
