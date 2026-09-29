SELECT DISTINCT city, country
FROM suppliers
WHERE LOWER(supplier_name) LIKE '%oky%'
ORDER BY city;
