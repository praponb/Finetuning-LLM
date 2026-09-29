SELECT id, city, supplier_name
FROM suppliers
WHERE supplier_name LIKE '%Glob%' OR UPPER(TRIM(supplier_name)) = 'TOKYO TRADERS'
