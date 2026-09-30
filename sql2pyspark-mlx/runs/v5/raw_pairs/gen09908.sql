SELECT revenue AS value
FROM store_sales
WHERE store_id IN (1, 2, 5, 6)
UNION
SELECT amount AS value
FROM orders;
