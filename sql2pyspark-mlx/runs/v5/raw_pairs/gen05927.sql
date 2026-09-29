SELECT
    *,
    LAG(rating) OVER w_ordered AS prev_rating,
    LEAD(rating) OVER w_ordered AS next_rating,
    ROW_NUMBER() OVER w_ordered AS seq_no,
    FIRST_VALUE(rating, TRUE) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating
FROM suppliers
WINDOW w_ordered AS (PARTITION BY city ORDER BY rating, id)
