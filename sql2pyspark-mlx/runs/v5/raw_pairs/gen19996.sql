SELECT
    id,
    country,
    rating,
    MAX(rating) OVER (PARTITION BY country) AS country_max_rating
FROM suppliers
WHERE supplier_name LIKE 'To%'
ORDER BY country, country_max_rating;
