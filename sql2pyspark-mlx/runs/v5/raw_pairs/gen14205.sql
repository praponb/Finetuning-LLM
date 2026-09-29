SELECT
    id,
    city,
    rating,
    LEAD(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating
FROM suppliers
ORDER BY 4 DESC;
