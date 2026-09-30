SELECT city
FROM customers
INTERSECT
SELECT city
FROM stores
WHERE region IN ('Central', 'South', 'East')
ORDER BY city;
