SELECT
    id,
    supplier_name,
    city,
    CONCAT(city, ' ', supplier_name) AS city_supplier_name
FROM suppliers
WHERE supplier_name LIKE 'G%' OR supplier_name LIKE '%Global%'
