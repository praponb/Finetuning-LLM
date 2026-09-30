SELECT
    id,
    supplier_name,
    city,
    rating,
    INITCAP(TRIM(city)) AS city_title
FROM suppliers
WHERE rating >= 3.1 AND rating > 3.1
ORDER BY rating, supplier_name;
