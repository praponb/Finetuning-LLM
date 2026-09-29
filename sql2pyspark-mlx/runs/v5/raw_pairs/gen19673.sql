SELECT
    id,
    country,
    rating,
    LAG(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) AS prev_rating,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_pct_rank
FROM suppliers
