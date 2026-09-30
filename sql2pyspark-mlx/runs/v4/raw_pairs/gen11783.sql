SELECT city, 'customer' source
FROM customers
UNION ALL
SELECT city, 'supplier' source
FROM suppliers
ORDER BY city
