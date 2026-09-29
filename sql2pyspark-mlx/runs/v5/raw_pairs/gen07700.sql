SELECT shipping_fee
FROM orders
WHERE amount >= 1460
UNION ALL
SELECT amount shipping_fee
FROM payments
WHERE status = 'FAILED'
