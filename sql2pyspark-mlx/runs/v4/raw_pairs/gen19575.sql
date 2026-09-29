SELECT country, city
FROM customers
WHERE is_active
INTERSECT
SELECT country, city
FROM suppliers;
