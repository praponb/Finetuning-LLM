SELECT supplier_name
FROM suppliers
WHERE city = 'Singapore'
UNION ALL
SELECT supplier_name
FROM suppliers
WHERE city IS NULL
