SELECT
    id,
    country,
    rating,
    SUM(rating) OVER (PARTITION BY country ORDER BY rating, id) AS running_rating
FROM suppliers
