SELECT amount
FROM transactions
WHERE account_id IN (9, 24, 34, 40)
INTERSECT
SELECT price
FROM products;
