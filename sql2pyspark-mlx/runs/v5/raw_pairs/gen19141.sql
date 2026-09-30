SELECT cost, 'product' AS origin
FROM products
UNION ALL
SELECT amount, 'payment' AS origin
FROM payments
WHERE amount >= 1070
ORDER BY cost
