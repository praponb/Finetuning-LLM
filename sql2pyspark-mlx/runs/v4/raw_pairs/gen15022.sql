SELECT status, 'order' AS source
FROM orders
WHERE shipping_fee < 14
UNION
SELECT status, 'payment' AS source
FROM payments
WHERE amount <= 970
