SELECT amount
FROM transactions
WHERE account_id IN (14, 34)
UNION
SELECT price
FROM products
ORDER BY amount;
