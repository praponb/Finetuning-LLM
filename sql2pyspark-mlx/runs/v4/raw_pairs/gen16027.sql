SELECT country, city, 'customer' source
FROM customers
UNION
SELECT country, city, 'supplier' source
FROM suppliers
ORDER BY country
