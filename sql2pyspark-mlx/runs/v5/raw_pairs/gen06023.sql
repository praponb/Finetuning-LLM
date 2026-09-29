SELECT country
FROM suppliers
WHERE UPPER(supplier_name) LIKE '%URO%'
UNION
SELECT country
FROM suppliers
WHERE supplier_name NOT LIKE 'Tok%'
