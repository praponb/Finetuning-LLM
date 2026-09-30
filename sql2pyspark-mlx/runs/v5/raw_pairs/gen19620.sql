SELECT
    id,
    country,
    rating,
    city,
    rating - LAG(rating, 1, 0) OVER (PARTITION BY country ORDER BY rating, id) AS rating_change,
    LEAD(rating, 1, 0) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating
FROM suppliers;
