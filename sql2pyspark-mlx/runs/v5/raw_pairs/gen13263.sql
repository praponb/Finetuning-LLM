SELECT
    id,
    country,
    rating,
    city,
    SUM(rating) OVER (PARTITION BY country ORDER BY rating, id) AS running_rating,
    SUM(rating) OVER (PARTITION BY city) AS city_sum_rating
FROM suppliers
ORDER BY rating
