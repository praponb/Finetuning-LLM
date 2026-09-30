SELECT *
FROM suppliers
WHERE (city IN ('London', 'Tokyo') AND supplier_name LIKE 'E%') OR UPPER(supplier_name) LIKE '%URO%'
