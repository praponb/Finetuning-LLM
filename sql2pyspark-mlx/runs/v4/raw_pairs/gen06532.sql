SELECT
    *,
    LAST_VALUE(rating) IGNORE NULLS OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
    NTILE(10) OVER (PARTITION BY country, city ORDER BY rating, id) AS rating_bucket
FROM suppliers
