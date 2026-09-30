SELECT price
FROM products
UNION
SELECT amount
FROM payments
ORDER BY price
