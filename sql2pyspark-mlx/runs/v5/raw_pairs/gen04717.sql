SELECT
    id,
    city,
    supplier_name,
    rating,
    UPPER(city) AS city_upper,
    CEIL(rating) AS rating_whole
FROM suppliers
WHERE supplier_name LIKE '%Tokyo%';
