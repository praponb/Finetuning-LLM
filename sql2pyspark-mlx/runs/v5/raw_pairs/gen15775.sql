SELECT 'order' AS source, status
FROM orders
WHERE store_id = 39
UNION
SELECT 'payment' AS source, status
FROM payments;
