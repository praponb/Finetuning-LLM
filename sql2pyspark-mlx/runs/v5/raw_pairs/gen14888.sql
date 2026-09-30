SELECT id, supplier_name, country
FROM suppliers
WHERE supplier_name LIKE 'Glo%'
UNION
SELECT id, supplier_name, country
FROM suppliers
WHERE city IS NOT NULL
ORDER BY id
