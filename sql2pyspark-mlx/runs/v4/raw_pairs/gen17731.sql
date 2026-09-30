SELECT
    id,
    city,
    rating,
    LAG(rating, 1, 0) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating
FROM suppliers
WHERE rating < 2.5
ORDER BY prev_rating DESC
