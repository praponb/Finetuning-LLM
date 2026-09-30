SELECT
    id,
    country,
    CONCAT(city, '/', country) AS city_country,
    COALESCE(supplier_name, 'unassigned') AS supplier_name_filled
FROM suppliers
