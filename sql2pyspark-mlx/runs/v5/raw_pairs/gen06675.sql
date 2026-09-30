SELECT
    country,
    supplier_name,
    rating,
    LOWER(city) AS city_lower
FROM suppliers
WHERE rating < 3.7
ORDER BY city_lower DESC, 1 ASC
