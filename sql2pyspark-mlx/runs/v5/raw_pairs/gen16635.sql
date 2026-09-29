SELECT
    id,
    city,
    rating,
    LAG(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rating_row
FROM suppliers
