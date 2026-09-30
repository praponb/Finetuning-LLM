SELECT
    id,
    rating,
    city,
    LAG(rating, 1) OVER (ORDER BY rating, id) prev_rating,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) dense_rnk
FROM suppliers
