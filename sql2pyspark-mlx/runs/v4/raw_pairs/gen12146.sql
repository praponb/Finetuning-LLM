SELECT country, supplier_name
FROM suppliers
WHERE rating > 4.1
UNION ALL
SELECT country, supplier_name
FROM suppliers
WHERE city = 'Berlin'
ORDER BY supplier_name;
