SELECT 'product' AS source, price
FROM products
UNION
SELECT 'payment' AS source, amount
FROM payments
WHERE amount <= 1530;
