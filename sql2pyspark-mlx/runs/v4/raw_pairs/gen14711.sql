SELECT
    id,
    country,
    rating,
    SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
    AVG(rating) OVER (PARTITION BY country) AS country_avg_rating
FROM suppliers;
