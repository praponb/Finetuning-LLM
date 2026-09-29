SELECT
    id,
    country,
    rating,
    MIN(rating) OVER (PARTITION BY country) AS country_min_rating,
    SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
FROM suppliers
ORDER BY running_rating DESC;
