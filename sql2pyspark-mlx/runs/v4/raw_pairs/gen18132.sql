SELECT city
FROM customers
WHERE age < 34
EXCEPT
SELECT city
FROM stores
ORDER BY city;
