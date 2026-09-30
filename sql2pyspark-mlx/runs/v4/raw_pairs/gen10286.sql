SELECT DISTINCT city, country
FROM suppliers
WHERE supplier_name IS NOT NULL
ORDER BY city, country DESC;
