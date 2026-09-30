SELECT amount
FROM transactions
WHERE amount > 1100
EXCEPT
SELECT revenue AS amount
FROM store_sales
ORDER BY amount
