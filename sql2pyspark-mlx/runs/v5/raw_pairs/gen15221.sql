WITH scored AS (
    SELECT
        id,
        country,
        city,
        MIN(rating) OVER w_running last4_min_rating,
        MAX(rating) OVER w_running last4_max_rating,
        AVG(rating) OVER w_running last4_avg_rating
    FROM suppliers
    WHERE rating < 3.7
    WINDOW w_running AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    country,
    city,
    last4_min_rating,
    last4_max_rating,
    last4_avg_rating
FROM scored
ORDER BY last4_avg_rating DESC;
