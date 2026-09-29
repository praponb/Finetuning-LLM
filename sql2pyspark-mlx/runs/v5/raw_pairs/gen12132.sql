SELECT *, LENGTH(city) AS city_length, INITCAP(TRIM(supplier_name)) AS supplier_name_title
FROM suppliers;
