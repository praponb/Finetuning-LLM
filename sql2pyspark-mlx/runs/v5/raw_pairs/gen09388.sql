SELECT city
FROM suppliers
INTERSECT
SELECT city
FROM stores
ORDER BY city DESC;
