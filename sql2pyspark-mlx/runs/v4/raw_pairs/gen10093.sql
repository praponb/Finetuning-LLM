SELECT
    *,
    MIN(rating) OVER (PARTITION BY country) country_min_rating,
    MIN(rating) OVER (PARTITION BY city) city_min_rating,
    NTILE(5) OVER (PARTITION BY country ORDER BY rating DESC, id) rating_bucket
FROM suppliers;
