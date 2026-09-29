SELECT
    id,
    city,
    rating,
    LEAD(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) next_rating
FROM suppliers
ORDER BY id DESC;
