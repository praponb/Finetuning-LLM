SELECT
    id,
    city,
    rating,
    ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
    RANK() OVER (PARTITION BY country ORDER BY rating NULLS LAST) AS rating_rank,
    LEAD(rating, 2, 0) OVER (ORDER BY rating, id) AS next_rating,
    LAG(rating, 2) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating,
    AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rating_avg_2
FROM suppliers
