SELECT
    id,
    city,
    rating,
    LAG(rating, 2) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating
FROM suppliers
WHERE rating < 3.5;
