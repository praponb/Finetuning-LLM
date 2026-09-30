SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rating_avg_6_prior,
    LAST_VALUE(rating) IGNORE NULLS OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
    AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rating_avg_2_prior
FROM suppliers;
