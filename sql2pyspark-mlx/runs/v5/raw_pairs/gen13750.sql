SELECT id, city, SUBSTRING(country, 1, 2) AS country_prefix
FROM suppliers
WHERE supplier_name IS NOT NULL;
