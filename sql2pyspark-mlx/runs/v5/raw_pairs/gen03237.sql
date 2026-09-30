SELECT
    id,
    rating,
    MAX(rating) OVER w_part AS last4_max_rating,
    MIN(rating) OVER w_part AS last4_min_rating,
    SUM(rating) OVER w_part AS last4_sum_rating
FROM suppliers
WHERE city <> 'London'
WINDOW w_part AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
