SELECT
    id,
    supplier_name,
    SUBSTRING(city, 1, 4) AS city_prefix,
    LENGTH(supplier_name) AS supplier_name_length
FROM suppliers
WHERE city = 'Chiang Mai';
