SELECT id, country, city
FROM suppliers
WHERE supplier_name LIKE '%City%' AND LENGTH(supplier_name) >= 5
ORDER BY country DESC;
