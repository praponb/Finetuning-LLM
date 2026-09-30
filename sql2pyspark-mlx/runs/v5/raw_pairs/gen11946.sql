SELECT
    id,
    city,
    AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rating_avg_6_prior,
    PERCENT_RANK() OVER (ORDER BY rating DESC NULLS FIRST) AS rating_percent_rank,
    NTILE(3) OVER (PARTITION BY city, country ORDER BY rating, id) AS rating_bucket
FROM suppliers;
