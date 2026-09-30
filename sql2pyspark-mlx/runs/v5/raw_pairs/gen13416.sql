SELECT
    id,
    city,
    CONCAT(supplier_name, '/', city) AS supplier_name_city,
    LENGTH(city) AS city_length
FROM suppliers
WHERE rating BETWEEN 2.6 AND 2.8 AND country IN ('DE', 'JP')
ORDER BY city_length DESC, supplier_name_city
