SELECT shipping_fee
FROM orders
WHERE amount <= 1070
INTERSECT
SELECT amount
FROM payments
WHERE amount < 870;
