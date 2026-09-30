SELECT revenue AS value
FROM store_sales
WHERE revenue <= 1700
EXCEPT
SELECT amount AS value
FROM transactions
ORDER BY value;
