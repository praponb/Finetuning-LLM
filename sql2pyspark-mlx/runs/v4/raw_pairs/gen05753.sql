SELECT status, 'order' AS source
FROM orders
WHERE amount > 1130
UNION ALL
SELECT status, 'payment' AS source
FROM payments
