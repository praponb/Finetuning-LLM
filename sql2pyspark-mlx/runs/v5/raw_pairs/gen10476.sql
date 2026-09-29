SELECT cost, 'product' AS origin
FROM products
UNION
SELECT amount, 'transaction' AS origin
FROM transactions
ORDER BY origin DESC;
