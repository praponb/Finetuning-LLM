SELECT country, city
FROM customers
UNION ALL
SELECT country, city
FROM suppliers
WHERE country IN ('DE', 'SG', 'JP')
ORDER BY city;
