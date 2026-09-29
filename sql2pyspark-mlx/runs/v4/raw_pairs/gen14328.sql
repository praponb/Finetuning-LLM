SELECT
    country,
    city,
    rating,
    LOWER(country) AS country_lower
FROM suppliers
WHERE rating < 2.5;
