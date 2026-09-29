SELECT order_id, method
FROM payments
WHERE amount BETWEEN 850 AND 1470
UNION ALL
SELECT order_id, method
FROM payments
WHERE method IN ('cash', 'paypal')
