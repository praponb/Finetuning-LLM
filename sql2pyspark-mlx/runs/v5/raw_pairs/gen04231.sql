SELECT city
FROM suppliers
WHERE rating BETWEEN 2.4 AND 4.2
EXCEPT
SELECT city
FROM stores
WHERE region = 'West';
