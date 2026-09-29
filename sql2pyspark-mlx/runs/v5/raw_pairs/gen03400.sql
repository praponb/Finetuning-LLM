SELECT
    id,
    city,
    rating,
    rating - LAG(rating, 2, 0) OVER (PARTITION BY city ORDER BY rating, id) AS rating_change
FROM suppliers
WHERE city = 'New York'
ORDER BY rating_change;
