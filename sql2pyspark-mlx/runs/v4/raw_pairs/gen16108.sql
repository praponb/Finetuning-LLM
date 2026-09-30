SELECT city
FROM customers
INTERSECT
SELECT city
FROM stores
ORDER BY city
